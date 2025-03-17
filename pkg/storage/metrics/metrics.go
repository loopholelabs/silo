package metrics

import (
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
)

type SiloMetrics interface {
	Shutdown()
	RemoveAllID(id string)

	AddSyncer(id string, name string, sync *migrator.Syncer)
	RemoveSyncer(id string, name string)

	AddMigrator(id string, name string, mig *migrator.Migrator)
	RemoveMigrator(id string, name string)

	AddProtocol(id string, name string, proto *protocol.RW)
	RemoveProtocol(id string, name string)

	AddToProtocol(id string, name string, proto *protocol.ToProtocol)
	RemoveToProtocol(id string, name string)

	AddFromProtocol(id string, name string, proto *protocol.FromProtocol)
	RemoveFromProtocol(id string, name string)

	AddS3Storage(id string, name string, s3 *sources.S3Storage)
	RemoveS3Storage(id string, name string)

	AddDirtyTracker(id string, name string, dt *dirtytracker.Remote)
	RemoveDirtyTracker(id string, name string)

	AddVolatilityMonitor(id string, name string, vm *volatilitymonitor.VolatilityMonitor)
	RemoveVolatilityMonitor(id string, name string)

	AddMetrics(id string, name string, mm *modules.Metrics)
	RemoveMetrics(id string, name string)

	AddNBD(id string, name string, mm *expose.ExposedStorageNBDNL)
	RemoveNBD(id string, name string)

	AddWaitingCache(id string, name string, wc *waitingcache.Remote)
	RemoveWaitingCache(id string, name string)
}
