package devicegroup

import (
	"context"
	"errors"
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
)

const volatilityExpiry = 30 * time.Minute
const defaultBlockSize = 1024 * 1024
const maxDirtyHistory = 32

var errNotSetup = errors.New("toProtocol not setup")

type DeviceGroup struct {
	log     types.Logger
	met     metrics.SiloMetrics
	devices []*DeviceInformation
}

type DeviceInformation struct {
	size           uint64
	blockSize      uint64
	numBlocks      int
	schema         *config.DeviceSchema
	prov           storage.Provider
	storage        storage.LockableProvider
	exp            storage.ExposedStorage
	volatility     *volatilitymonitor.VolatilityMonitor
	dirtyLocal     *dirtytracker.Local
	dirtyRemote    *dirtytracker.Remote
	to             *protocol.ToProtocol
	orderer        *blocks.PriorityBlockOrder
	migrator       *migrator.Migrator
	migrationError chan error
}

func New(ds []*config.DeviceSchema, log types.Logger, met metrics.SiloMetrics) (*DeviceGroup, error) {
	dg := &DeviceGroup{
		log:     log,
		met:     met,
		devices: make([]*DeviceInformation, 0),
	}

	for _, s := range ds {
		prov, exp, err := device.NewDeviceWithLoggingMetrics(s, log, met)
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
			met.AddMetrics(s.Name, mlocal)
			if exp != nil {
				met.AddNBD(s.Name, exp.(*expose.ExposedStorageNBDNL))
			}
			met.AddDirtyTracker(s.Name, dirtyRemote)
			met.AddVolatilityMonitor(s.Name, vmonitor)
		}

		dg.devices = append(dg.devices, &DeviceInformation{
			size:        local.Size(),
			blockSize:   uint64(blockSize),
			numBlocks:   totalBlocks,
			schema:      s,
			prov:        prov,
			storage:     local,
			exp:         exp,
			volatility:  vmonitor,
			dirtyLocal:  dirtyLocal,
			dirtyRemote: dirtyRemote,
			orderer:     orderer,
		})
	}

	if log != nil {
		log.Debug().Int("devices", len(dg.devices)).Msg("created device group")
	}
	return dg, nil
}

func (dg *DeviceGroup) GetProvider(index int) storage.Provider {
	return dg.devices[index].storage
}

func (dg *DeviceGroup) StartMigrationTo(pro protocol.Protocol) error {
	var e error

	for index, d := range dg.devices {
		d.to = protocol.NewToProtocol(d.prov.Size(), uint32(index), pro)
		d.to.SetCompression(true)

		if dg.met != nil {
			dg.met.AddToProtocol(d.schema.Name, d.to)
		}

		schema := string(d.schema.Encode())

		err := d.to.SendDevInfo(d.schema.Name, uint32(d.blockSize), schema)
		if err != nil {
			if dg.log != nil {
				dg.log.Error().Str("schema", schema).Msg("could not send DevInfo")
			}
			e = errors.Join(e, err)
		}
	}
	return e
}

// This will Migrate all devices to the 'to' setup in SendDevInfo stage.
func (dg *DeviceGroup) MigrateAll(maxConcurrency int, progressHandler func(i int, p *migrator.MigrationProgress)) error {
	for _, d := range dg.devices {
		if d.to == nil {
			return errNotSetup
		}
	}

	ctime := time.Now()

	if dg.log != nil {
		dg.log.Debug().Int("devices", len(dg.devices)).Msg("migrating device group")
	}

	// Add up device sizes, so we can allocate the concurrency proportionally
	totalSize := uint64(0)
	for _, d := range dg.devices {
		totalSize += d.size
	}

	// We need at least this much...
	if maxConcurrency < len(dg.devices) {
		maxConcurrency = len(dg.devices)
	}
	// We will allocate each device at least ONE...
	maxConcurrency -= len(dg.devices)

	for index, d := range dg.devices {
		concurrency := 1 + (uint64(maxConcurrency) * d.size / totalSize)
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
			err := d.to.HandleNeedAt(func(offset int64, length int32) {
				if dg.log != nil {
					dg.log.Debug().
						Int64("offset", offset).
						Int32("length", length).
						Int("dev", index).
						Str("name", d.schema.Name).
						Msg("NeedAt for device")
				}
				// Prioritize blocks
				endOffset := uint64(offset + int64(length))
				if endOffset > d.size {
					endOffset = d.size
				}

				startBlock := int(offset / int64(d.blockSize))
				endBlock := int((endOffset-1)/d.blockSize) + 1
				for b := startBlock; b < endBlock; b++ {
					d.orderer.PrioritiseBlock(b)
				}
			})
			setMigrationError(err)
		}()

		go func() {
			err := d.to.HandleDontNeedAt(func(offset int64, length int32) {
				if dg.log != nil {
					dg.log.Debug().
						Int64("offset", offset).
						Int32("length", length).
						Int("dev", index).
						Str("name", d.schema.Name).
						Msg("DontNeedAt for device")
				}
				// Deprioritize blocks
				endOffset := uint64(offset + int64(length))
				if endOffset > d.size {
					endOffset = d.size
				}

				startBlock := int(offset / int64(d.blockSize))
				endBlock := int((endOffset-1)/d.blockSize) + 1
				for b := startBlock; b < endBlock; b++ {
					d.orderer.Remove(b)
				}
			})
			setMigrationError(err)
		}()

		cfg := migrator.NewConfig()
		cfg.Logger = dg.log
		cfg.BlockSize = int(d.blockSize)
		cfg.Concurrency = map[int]int{
			storage.BlockTypeAny: int(concurrency),
		}
		cfg.LockerHandler = func() {
			setMigrationError(d.to.SendEvent(&packets.Event{Type: packets.EventPreLock}))
			d.storage.Lock()
			setMigrationError(d.to.SendEvent(&packets.Event{Type: packets.EventPostLock}))
		}
		cfg.UnlockerHandler = func() {
			setMigrationError(d.to.SendEvent(&packets.Event{Type: packets.EventPreUnlock}))
			d.storage.Unlock()
			setMigrationError(d.to.SendEvent(&packets.Event{Type: packets.EventPostUnlock}))
		}
		cfg.ErrorHandler = func(_ *storage.BlockInfo, err error) {
			setMigrationError(err)
		}
		cfg.ProgressHandler = func(p *migrator.MigrationProgress) {
			progressHandler(index, p)
		}
		mig, err := migrator.NewMigrator(d.dirtyRemote, d.to, d.orderer, cfg)
		if err != nil {
			return err
		}
		d.migrator = mig
		if dg.met != nil {
			dg.met.AddMigrator(d.schema.Name, mig)
		}
		if dg.log != nil {
			dg.log.Debug().
				Uint64("concurrency", concurrency).
				Int("index", index).
				Str("name", d.schema.Name).
				Msg("Setup migrator")
		}
	}

	errs := make(chan error, len(dg.devices))

	// Now start them all migrating, and collect err
	for _, d := range dg.devices {
		go func() {
			err := d.migrator.Migrate(d.numBlocks)
			errs <- err
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
		err := d.migrator.WaitForCompletion()
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
	PreGetDirty      func(index int, to *protocol.ToProtocol, dirtyHistory []int)
	PostGetDirty     func(index int, to *protocol.ToProtocol, dirtyHistory []int, blocks []uint)
	PostMigrateDirty func(index int, to *protocol.ToProtocol, dirtyHistory []int) bool
	Completed        func(index int, to *protocol.ToProtocol)
}

func (dg *DeviceGroup) MigrateDirty(hooks *MigrateDirtyHooks) error {
	errs := make(chan error, len(dg.devices))

	for index, d := range dg.devices {
		go func() {
			dirtyHistory := make([]int, 0)

			for {
				if hooks != nil && hooks.PreGetDirty != nil {
					hooks.PreGetDirty(index, d.to, dirtyHistory)
				}

				blocks := d.migrator.GetLatestDirty()
				if dg.log != nil {
					dg.log.Debug().
						Int("blocks", len(blocks)).
						Int("index", index).
						Str("name", d.schema.Name).
						Msg("migrating dirty blocks")
				}

				dirtyHistory = append(dirtyHistory, len(blocks))
				// Cap it at a certain MAX LENGTH
				if len(dirtyHistory) > maxDirtyHistory {
					dirtyHistory = dirtyHistory[1:]
				}

				if hooks != nil && hooks.PostGetDirty != nil {
					hooks.PostGetDirty(index, d.to, dirtyHistory, blocks)
				}

				if len(blocks) == 0 {
					break
				}

				err := d.to.DirtyList(int(d.blockSize), blocks)
				if err != nil {
					errs <- err
					return
				}

				err = d.migrator.MigrateDirty(blocks)
				if err != nil {
					errs <- err
					return
				}

				if hooks != nil && hooks.PostMigrateDirty != nil {
					if hooks.PostMigrateDirty(index, d.to, dirtyHistory) {
						break // PostMigrateDirty returned true, which means stop doing any dirty loop business.
					}
				}
			}

			err := d.migrator.WaitForCompletion()
			if err != nil {
				errs <- err
				return
			}

			err = d.to.SendEvent(&packets.Event{Type: packets.EventCompleted})
			if err != nil {
				errs <- err
				return
			}

			if hooks != nil && hooks.Completed != nil {
				hooks.Completed(index, d.to)
			}

			if dg.log != nil {
				dg.log.Debug().
					Int("index", index).
					Str("name", d.schema.Name).
					Msg("migrating dirty blocks completed")
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

func (dg *DeviceGroup) CloseAll() error {
	if dg.log != nil {
		dg.log.Debug().Int("devices", len(dg.devices)).Msg("close device group")
	}

	var e error
	for _, d := range dg.devices {
		// Unlock the storage so nothing blocks here...
		// If we don't unlock there may be pending nbd writes that can't be completed.
		d.storage.Unlock()

		err := d.prov.Close()
		if err != nil {
			if dg.log != nil {
				dg.log.Error().Err(err).Msg("error closing device group storage provider")
			}
			e = errors.Join(e, err)
		}
		if d.exp != nil {
			err = d.exp.Shutdown()
			if err != nil {
				if dg.log != nil {
					dg.log.Error().Err(err).Msg("error closing device group exposed storage")
				}
				e = errors.Join(e, err)
			}
		}
	}
	return e
}
