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
	AddSyncer(name string, sync *migrator.Syncer)
	RemoveSyncer(name string)

	AddMigrator(name string, mig *migrator.Migrator)
	RemoveMigrator(name string)

	AddProtocol(name string, proto *protocol.RW)
	RemoveProtocol(name string)

	AddToProtocol(name string, proto *protocol.ToProtocol)
	RemoveToProtocol(name string)

	AddFromProtocol(name string, proto *protocol.FromProtocol)
	RemoveFromProtocol(name string)

	AddS3Storage(name string, s3 *sources.S3Storage)
	RemoveS3Storage(name string)

	AddDirtyTracker(name string, dt *dirtytracker.Remote)
	RemoveDirtyTracker(name string)

	AddVolatilityMonitor(name string, vm *volatilitymonitor.VolatilityMonitor)
	RemoveVolatilityMonitor(name string)

	AddMetrics(name string, mm *modules.Metrics)
	RemoveMetrics(name string)

	AddNBD(name string, mm *expose.ExposedStorageNBDNL)
	RemoveNBD(name string)

	AddWaitingCache(name string, wc *waitingcache.Remote)
	RemoveWaitingCache(name string)
}
