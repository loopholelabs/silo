package devicegroup

import (
	"context"
	"time"

	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
)

func NewFromSchema(instanceID string, ds []*config.DeviceSchema, createWC bool, log types.Logger, met metrics.SiloMetrics) (*DeviceGroup, error) {
	dg := &DeviceGroup{
		log:        log,
		met:        met,
		instanceID: instanceID,
		devices:    make([]*DeviceInformation, 0),
		progress:   make(map[string]*migrator.MigrationProgress),
	}

	for _, s := range ds {
		prov, exp, err := device.NewDeviceWithLoggingMetrics(s, log, met, instanceID)
		if err != nil {
			if log != nil {
				log.Error().Err(err).Str("schema", string(s.Encode())).Msg("could not create device")
			}
			// We should try to close / shutdown any successful devices we created here...
			// But it's likely to be critical.
			dg.CloseAll()
			return nil, err
		}

		blockSize := int(s.ByteBlockSize())
		if blockSize == 0 {
			blockSize = defaultBlockSize
		}

		var waitingCacheLocal *waitingcache.Local
		var waitingCacheRemote *waitingcache.Remote
		if createWC {
			waitingCacheLocal, waitingCacheRemote = waitingcache.NewWaitingCacheWithLogger(prov, blockSize, dg.log)
			prov = waitingCacheLocal
		}

		local := modules.NewLockable(prov)
		mlocal := modules.NewMetrics(local)
		dirtyLocal, dirtyRemote := dirtytracker.NewDirtyTracker(mlocal, blockSize)
		vmonitor := volatilitymonitor.NewVolatilityMonitor(dirtyLocal, blockSize, volatilityExpiry)

		totalBlocks := (int(local.Size()) + blockSize - 1) / blockSize
		orderer := blocks.NewPriorityBlockOrder(totalBlocks, vmonitor)
		orderer.AddAll()

		if exp != nil {
			exp.SetProvider(vmonitor)
		}

		// Add to metrics if given.
		if met != nil {
			met.AddMetrics(dg.instanceID, s.Name, mlocal)
			if exp != nil {
				met.AddNBD(dg.instanceID, s.Name, exp.(*expose.ExposedStorageNBDNL))
			}
			met.AddDirtyTracker(dg.instanceID, s.Name, dirtyRemote)
			met.AddVolatilityMonitor(dg.instanceID, s.Name, vmonitor)
		}

		dg.devices = append(dg.devices, &DeviceInformation{
			Size:               local.Size(),
			BlockSize:          uint64(blockSize),
			NumBlocks:          totalBlocks,
			Schema:             s,
			Prov:               prov,
			Storage:            local,
			Exp:                exp,
			Volatility:         vmonitor,
			DirtyLocal:         dirtyLocal,
			DirtyRemote:        dirtyRemote,
			Orderer:            orderer,
			WaitingCacheLocal:  waitingCacheLocal,
			WaitingCacheRemote: waitingCacheRemote,
		})

		// Set these two at least, so we know *something* about every device in progress handler.
		dg.progress[s.Name] = &migrator.MigrationProgress{
			BlockSize:   blockSize,
			TotalBlocks: totalBlocks,
		}
	}

	if log != nil {
		log.Debug().Int("devices", len(dg.devices)).Msg("created device group")
	}
	return dg, nil
}

func (dg *DeviceGroup) StartMigrationTo(pro protocol.Protocol, compression bool) error {
	// We will use dev 0 to communicate
	dg.controlProtocol = pro

	// First lets setup the ToProtocol
	for index, d := range dg.devices {
		d.To = protocol.NewToProtocol(d.Prov.Size(), uint32(index+1), pro)
		d.To.SetCompression(compression)

		if dg.met != nil {
			dg.met.AddToProtocol(dg.instanceID, d.Schema.Name, d.To)
		}
	}

	// Now package devices up into a single DeviceGroupInfo
	dgi := &packets.DeviceGroupInfo{
		Devices: make(map[int]*packets.DevInfo),
	}

	for index, d := range dg.devices {
		di := &packets.DevInfo{
			Size:      d.Prov.Size(),
			BlockSize: uint32(d.BlockSize),
			Name:      d.Schema.Name,
			Schema:    string(d.Schema.EncodeAsBlock()),
		}
		dgi.Devices[index+1] = di
	}

	// Send the single DeviceGroupInfo packet down our control channel 0
	dgiData := packets.EncodeDeviceGroupInfo(dgi)
	_, err := dg.controlProtocol.SendPacket(0, protocol.IDPickAny, dgiData, protocol.UrgencyUrgent)

	return err
}

// This will Migrate all devices to the 'to' setup in SendDevInfo stage.
func (dg *DeviceGroup) MigrateAll(maxConcurrency int, progressHandler func(p map[string]*migrator.MigrationProgress)) error {
	for _, d := range dg.devices {
		if d.To == nil {
			return errNotSetup
		}
	}

	// Check if the devices are actually all here?
	for _, d := range dg.devices {
		if d.WaitingCacheLocal != nil {
			wcMetrics := d.WaitingCacheLocal.GetMetrics()
			if wcMetrics.AvailableRemote < uint64(d.NumBlocks) {
				if dg.log != nil {
					dg.log.Warn().
						Str("name", d.Schema.Name).
						Uint64("availableRemoteBlocks", wcMetrics.AvailableRemote).
						Uint64("numBlocks", uint64(d.NumBlocks)).
						Msg("migrating away a possibly incomplete source")
				}
			}
		}
	}

	ctime := time.Now()

	if dg.log != nil {
		dg.log.Debug().Int("devices", len(dg.devices)).Msg("migrating device group")
	}

	// Add up device sizes, so we can allocate the concurrency proportionally
	totalSize := uint64(0)
	for _, d := range dg.devices {
		totalSize += d.Size
	}

	// We need at least this much...
	if maxConcurrency < len(dg.devices) {
		maxConcurrency = len(dg.devices)
	}
	// We will allocate each device at least ONE...
	maxConcurrency -= len(dg.devices)

	for index, d := range dg.devices {
		concurrency := 1 + (uint64(maxConcurrency) * d.Size / totalSize)
		d.migrationError = make(chan error, 1) // We will just hold onto the first error for now.

		setMigrationError := func(err error) {
			if err != nil && err != context.Canceled {
				select {
				case d.migrationError <- err:
				default:
				}
			}
		}

		// Setup d.to
		go func() {
			err := d.To.HandleNeedAt(func(offset int64, length int32) {
				if dg.log != nil {
					dg.log.Debug().
						Int64("offset", offset).
						Int32("length", length).
						Int("dev", index).
						Str("name", d.Schema.Name).
						Msg("NeedAt for device")
				}
				// Prioritize blocks
				endOffset := uint64(offset + int64(length))
				if endOffset > d.Size {
					endOffset = d.Size
				}

				startBlock := int(offset / int64(d.BlockSize))
				endBlock := int((endOffset-1)/d.BlockSize) + 1
				for b := startBlock; b < endBlock; b++ {
					d.Orderer.PrioritiseBlock(b)
				}
			})
			setMigrationError(err)
		}()

		go func() {
			err := d.To.HandleDontNeedAt(func(offset int64, length int32) {
				if dg.log != nil {
					dg.log.Debug().
						Int64("offset", offset).
						Int32("length", length).
						Int("dev", index).
						Str("name", d.Schema.Name).
						Msg("DontNeedAt for device")
				}
				// Deprioritize blocks
				endOffset := uint64(offset + int64(length))
				if endOffset > d.Size {
					endOffset = d.Size
				}

				startBlock := int(offset / int64(d.BlockSize))
				endBlock := int((endOffset-1)/d.BlockSize) + 1
				for b := startBlock; b < endBlock; b++ {
					d.Orderer.Remove(b)
				}
			})
			setMigrationError(err)
		}()

		cfg := migrator.NewConfig()
		cfg.Logger = dg.log
		cfg.BlockSize = int(d.BlockSize)
		cfg.Concurrency = map[int]int{
			storage.BlockTypeAny: int(concurrency),
		}
		cfg.LockerHandler = func() {
			//			setMigrationError(d.to.SendEvent(&packets.Event{Type: packets.EventPreLock}))
			// d.Storage.Lock()
			//			setMigrationError(d.to.SendEvent(&packets.Event{Type: packets.EventPostLock}))
		}
		cfg.UnlockerHandler = func() {
			//			setMigrationError(d.to.SendEvent(&packets.Event{Type: packets.EventPreUnlock}))
			// d.Storage.Unlock()
			//			setMigrationError(d.to.SendEvent(&packets.Event{Type: packets.EventPostUnlock}))
		}
		cfg.ErrorHandler = func(_ *storage.BlockInfo, err error) {
			setMigrationError(err)
		}
		cfg.ProgressHandler = func(p *migrator.MigrationProgress) {
			dg.progressLock.Lock()
			dg.progress[d.Schema.Name] = p
			if progressHandler != nil {
				progressHandler(dg.progress)
			}
			dg.progressLock.Unlock()
		}
		mig, err := migrator.NewMigrator(d.DirtyRemote, d.To, d.Orderer, cfg)
		if err != nil {
			return err
		}
		d.Migrator = mig
		if dg.met != nil {
			dg.met.AddMigrator(dg.instanceID, d.Schema.Name, mig)
		}
		if dg.log != nil {
			dg.log.Debug().
				Uint64("concurrency", concurrency).
				Int("index", index).
				Str("name", d.Schema.Name).
				Msg("Setup migrator")
		}
	}

	errs := make(chan error, len(dg.devices))

	// Now start them all migrating, and collect err
	for _, d := range dg.devices {
		go func() {
			migrateBlocks := d.NumBlocks
			unrequired := d.DirtyRemote.GetUnrequiredBlocks()
			alreadyBlocks := make([]uint32, 0)
			for _, b := range unrequired {
				d.Orderer.Remove(int(b))
				migrateBlocks--
				alreadyBlocks = append(alreadyBlocks, uint32(b))
			}

			err := d.To.SendYouAlreadyHave(d.BlockSize, alreadyBlocks)
			if err != nil {
				errs <- err
			} else {
				err = d.Migrator.Migrate(migrateBlocks)
				errs <- err
			}
		}()
	}

	// Check for error from Migrate, and then Wait for completion of all devices...
	for index := range dg.devices {
		migErr := <-errs
		if migErr != nil {
			if dg.log != nil {
				dg.log.Error().Err(migErr).Int("index", index).Msg("error migrating device group")
			}
			return migErr
		}
	}

	for index, d := range dg.devices {
		err := d.Migrator.WaitForCompletion()
		if err != nil {
			if dg.log != nil {
				dg.log.Error().Err(err).Int("index", index).Msg("error migrating device group waiting for completion")
			}
			return err
		}

		// Check for any migration error
		select {
		case err := <-d.migrationError:
			if dg.log != nil {
				dg.log.Error().Err(err).Int("index", index).Msg("error migrating device group from goroutines")
			}
			return err
		default:
		}
	}

	if dg.log != nil {
		dg.log.Debug().Int64("duration", time.Since(ctime).Milliseconds()).Int("devices", len(dg.devices)).Msg("migration of device group completed")
	}

	return nil
}

type MigrateDirtyHooks struct {
	PreGetDirty      func(name string) error
	PostGetDirty     func(name string, blocks []uint) (bool, error)
	PostMigrateDirty func(name string, blocks []uint) (bool, error)
	Completed        func(name string)
}

func (dg *DeviceGroup) MigrateDirty(hooks *MigrateDirtyHooks) error {
	// If StartMigrationTo or MigrateAll have not been called, return error.
	for _, d := range dg.devices {
		if d.To == nil || d.Migrator == nil {
			return errNotSetup
		}
	}

	errs := make(chan error, len(dg.devices))

	for index, d := range dg.devices {
		// First unlock the storage if it is locked due to a previous MigrateDirty call
		d.Storage.Unlock()

		go func() {
			for {
				if hooks != nil && hooks.PreGetDirty != nil {
					hooks.PreGetDirty(d.Schema.Name)
				}

				blocks := d.Migrator.GetLatestDirty()
				if dg.log != nil {
					dg.log.Debug().
						Int("blocks", len(blocks)).
						Int("index", index).
						Str("name", d.Schema.Name).
						Msg("migrating dirty blocks")
				}

				if hooks != nil && hooks.PostGetDirty != nil {
					cont, err := hooks.PostGetDirty(d.Schema.Name, blocks)
					if err != nil {
						errs <- err
						return
					}
					if !cont {
						break
					}
				}

				err := d.To.DirtyList(int(d.BlockSize), blocks)
				if err != nil {
					errs <- err
					return
				}

				err = d.Migrator.MigrateDirty(blocks)
				if err != nil {
					errs <- err
					return
				}

				if hooks != nil && hooks.PostMigrateDirty != nil {
					cont, err := hooks.PostMigrateDirty(d.Schema.Name, blocks)
					if err != nil {
						errs <- err
					}
					if !cont {
						break
					}
				}
			}

			err := d.Migrator.WaitForCompletion()
			if err != nil {
				errs <- err
				return
			}

			if hooks != nil && hooks.Completed != nil {
				hooks.Completed(d.Schema.Name)
			}

			errs <- nil
		}()
	}

	// Wait for all dirty migrations to complete
	// Check for any error and return it
	for range dg.devices {
		err := <-errs
		if err != nil {
			return err
		}
	}

	return nil
}

func (dg *DeviceGroup) Completed() error {
	for index, d := range dg.devices {
		err := d.To.SendEvent(&packets.Event{Type: packets.EventCompleted})
		if err != nil {
			return err
		}

		if dg.log != nil {
			dg.log.Debug().
				Int("index", index).
				Str("name", d.Schema.Name).
				Msg("migration completed")
		}
	}
	return nil
}

func (dg *DeviceGroup) SendCustomData(customData []byte) error {

	// Send the single TransferAuthority packet down our control channel 0
	taData := packets.EncodeEvent(&packets.Event{
		Type:          packets.EventCustom,
		CustomType:    0,
		CustomPayload: customData,
	})
	id, err := dg.controlProtocol.SendPacket(0, protocol.IDPickAny, taData, protocol.UrgencyUrgent)
	if err != nil {
		return err
	}

	// Wait for ack
	ackData, err := dg.controlProtocol.WaitForPacket(0, id)
	if err != nil {
		return err
	}

	return packets.DecodeEventResponse(ackData)
}
