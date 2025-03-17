package devicegroup

import (
	"context"
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
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
)

const volatilityExpiry = 30 * time.Minute
const defaultBlockSize = 1024 * 1024

var errNotSetup = errors.New("toProtocol not setup")

type DeviceGroup struct {
	instanceID        string
	log               types.Logger
	met               metrics.SiloMetrics
	ctx               context.Context
	devices           []*DeviceInformation
	controlProtocol   protocol.Protocol
	incomingDevicesCh chan bool
	readyDevicesCh    chan bool
	progressLock      sync.Mutex
	progress          map[string]*migrator.MigrationProgress
}

type DeviceInformation struct {
	Size               uint64
	BlockSize          uint64
	NumBlocks          int
	Schema             *config.DeviceSchema
	Prov               storage.Provider
	Storage            storage.LockableProvider
	Exp                storage.ExposedStorage
	Volatility         *volatilitymonitor.VolatilityMonitor
	DirtyLocal         *dirtytracker.Local
	DirtyRemote        *dirtytracker.Remote
	To                 *protocol.ToProtocol
	From               *protocol.FromProtocol
	Orderer            *blocks.PriorityBlockOrder
	Migrator           *migrator.Migrator
	migrationError     chan error
	WaitingCacheLocal  *waitingcache.Local
	WaitingCacheRemote *waitingcache.Remote
	EventHandler       func(e *packets.Event)
}

func (dg *DeviceGroup) GetDeviceSchema() []*config.DeviceSchema {
	s := make([]*config.DeviceSchema, 0)
	for _, di := range dg.devices {
		s = append(s, di.Schema)
	}
	return s
}

func (dg *DeviceGroup) GetAllNames() []string {
	names := make([]string, 0)
	for _, di := range dg.devices {
		names = append(names, di.Schema.Name)
	}
	return names
}

func (dg *DeviceGroup) GetDeviceInformationByName(name string) *DeviceInformation {
	for _, di := range dg.devices {
		if di.Schema.Name == name {
			return di
		}
	}
	return nil
}

func (dg *DeviceGroup) GetExposedDeviceByName(name string) storage.ExposedStorage {
	for _, di := range dg.devices {
		if di.Schema.Name == name && di.Exp != nil {
			return di.Exp
		}
	}
	return nil
}

func (dg *DeviceGroup) GetProviderByName(name string) storage.Provider {
	for _, di := range dg.devices {
		if di.Schema.Name == name {
			return di.Prov
		}
	}
	return nil
}

func (dg *DeviceGroup) GetBlockSizeByName(name string) int {
	for _, di := range dg.devices {
		if di.Schema.Name == name {
			return int(di.BlockSize)
		}
	}
	return -1
}

func (dg *DeviceGroup) CloseAll() error {
	if dg.log != nil {
		dg.log.Debug().Int("devices", len(dg.devices)).Msg("close device group")
	}

	var e error
	for _, d := range dg.devices {
		// Unlock the storage so nothing blocks here...
		// If we don't unlock there may be pending nbd writes that can't be completed.
		d.Storage.Unlock()

		err := d.Prov.Close()
		if err != nil {
			if dg.log != nil {
				dg.log.Error().Err(err).Msg("error closing device group storage provider")
			}
			e = errors.Join(e, err)
		}
		if d.Exp != nil {
			err = d.Exp.Shutdown()
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
