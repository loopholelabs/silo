package devicegroup

import (
	"errors"
	"sync"
	"time"

	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
)

const volatilityExpiry = 30 * time.Minute
const defaultBlockSize = 1024 * 1024
const maxDirtyHistory = 32

var errNotSetup = errors.New("toProtocol not setup")

type DeviceGroup struct {
	log               types.Logger
	met               metrics.SiloMetrics
	devices           []*DeviceInformation
	controlProtocol   protocol.Protocol
	incomingDevicesWg sync.WaitGroup
	progressLock      sync.Mutex
	progress          []*migrator.MigrationProgress
}

type DeviceInformation struct {
	size               uint64
	blockSize          uint64
	numBlocks          int
	schema             *config.DeviceSchema
	prov               storage.Provider
	storage            storage.LockableProvider
	exp                storage.ExposedStorage
	volatility         *volatilitymonitor.VolatilityMonitor
	dirtyLocal         *dirtytracker.Local
	dirtyRemote        *dirtytracker.Remote
	to                 *protocol.ToProtocol
	orderer            *blocks.PriorityBlockOrder
	migrator           *migrator.Migrator
	migrationError     chan error
	waitingCacheLocal  *waitingcache.Local
	waitingCacheRemote *waitingcache.Remote
}

func (dg *DeviceGroup) GetExposedDeviceByName(name string) string {
	for _, di := range dg.devices {
		if di.schema.Name == name && di.exp != nil {
			return di.exp.Device()
		}
	}
	return ""
}

func (dg *DeviceGroup) GetProviderByName(name string) storage.Provider {
	for _, di := range dg.devices {
		if di.schema.Name == name {
			return di.prov
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
