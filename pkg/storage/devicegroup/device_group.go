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
			// We should try to close / shutdown any successful devices we created here...
			// But it's likely to be critical.
			dg.CloseAll()
			return nil, err
		}

		blockSize := int(s.ByteBlockSize())

		local := modules.NewLockable(prov)
		mlocal := modules.NewMetrics(local)
		dirtyLocal, dirtyRemote := dirtytracker.NewDirtyTracker(mlocal, blockSize)
		vmonitor := volatilitymonitor.NewVolatilityMonitor(dirtyLocal, blockSize, volatilityExpiry)

		totalBlocks := (int(local.Size()) + blockSize - 1) / blockSize
		orderer := blocks.NewPriorityBlockOrder(totalBlocks, vmonitor)
		orderer.AddAll()

		exp.SetProvider(vmonitor)

		// Add to metrics if given.
		if met != nil {
			met.AddMetrics(s.Name, mlocal)
			met.AddNBD(s.Name, exp.(*expose.ExposedStorageNBDNL))
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
	return dg, nil
}

func (dg *DeviceGroup) GetProvider(index int) storage.Provider {
	return dg.devices[index].storage
}

func (dg *DeviceGroup) SendDevInfo(pro protocol.Protocol) error {
	var e error

	for index, d := range dg.devices {
		d.to = protocol.NewToProtocol(d.prov.Size(), uint32(index), pro)
		d.to.SetCompression(true)

		if dg.met != nil {
			dg.met.AddToProtocol(d.schema.Name, d.to)
		}

		schema := d.schema.Encode()
		err := d.to.SendDevInfo(d.schema.Name, uint32(d.schema.ByteBlockSize()), string(schema))
		if err != nil {
			e = errors.Join(e, err)
		}
	}
	return e
}

// This will Migrate all devices to the 'to' setup in SendDevInfo stage.
func (dg *DeviceGroup) MigrateAll(progressHandler func(i int, p *migrator.MigrationProgress)) error {
	// TODO: We can divide concurrency amongst devices depending on their size...
	concurrency := 100

	for index, d := range dg.devices {
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
			storage.BlockTypeAny: concurrency,
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
	}

	errs := make(chan error, len(dg.devices))

	// Now start them all migrating, and collect err
	for _, d := range dg.devices {
		go func() {
			errs <- d.migrator.Migrate(d.numBlocks)
		}()
	}

	// Check for error from Migrate, and then Wait for completion of all devices...
	for _, d := range dg.devices {
		migErr := <-errs
		if migErr != nil {
			return migErr
		}

		err := d.migrator.WaitForCompletion()
		if err != nil {
			return err
		}

		// Check for any migration error
		select {
		case err := <-d.migrationError:
			return err
		default:
		}
	}

	return nil
}

func (dg *DeviceGroup) CloseAll() error {
	var e error
	for _, d := range dg.devices {
		err := d.prov.Close()
		if err != nil {
			e = errors.Join(e, err)
		}
		if d.exp != nil {
			err = d.exp.Shutdown()
			if err != nil {
				e = errors.Join(e, err)
			}
		}
	}
	return e
}
