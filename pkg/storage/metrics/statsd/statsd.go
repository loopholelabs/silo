package statsd

import (
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"

	"github.com/smira/go-statsd"
)

type Metrics struct {
	client *statsd.Client
}

func NewMetrics() *Metrics {
	client := statsd.NewClient("localhost:8125",
		statsd.MaxPacketSize(1400),
		statsd.MetricPrefix("silo."))

	return &Metrics{
		client: client,
	}
}

func (m *Metrics) Shutdown() {}

func (m *Metrics) RemoveAllID(id string) {}

func (m *Metrics) AddSyncer(id string, name string, sync *migrator.Syncer) {}
func (m *Metrics) RemoveSyncer(id string, name string)                     {}

func (m *Metrics) AddMigrator(id string, name string, mig *migrator.Migrator) {}
func (m *Metrics) RemoveMigrator(id string, name string)                      {}

func (m *Metrics) AddProtocol(id string, name string, proto *protocol.RW) {}
func (m *Metrics) RemoveProtocol(id string, name string)                  {}

func (m *Metrics) AddToProtocol(id string, name string, proto *protocol.ToProtocol) {}
func (m *Metrics) RemoveToProtocol(id string, name string)                          {}

func (m *Metrics) AddFromProtocol(id string, name string, proto *protocol.FromProtocol) {}
func (m *Metrics) RemoveFromProtocol(id string, name string)                            {}

func (m *Metrics) AddS3Storage(id string, name string, s3 *sources.S3Storage) {}
func (m *Metrics) RemoveS3Storage(id string, name string)                     {}

func (m *Metrics) AddDirtyTracker(id string, name string, dt *dirtytracker.Remote) {}
func (m *Metrics) RemoveDirtyTracker(id string, name string)                       {}

func (m *Metrics) AddVolatilityMonitor(id string, name string, vm *volatilitymonitor.VolatilityMonitor) {
}
func (m *Metrics) RemoveVolatilityMonitor(id string, name string) {}

func (m *Metrics) AddMetrics(id string, name string, mm *modules.Metrics) {}
func (m *Metrics) RemoveMetrics(id string, name string)                   {}

func (m *Metrics) AddNBD(id string, name string, mm *expose.ExposedStorageNBDNL) {}
func (m *Metrics) RemoveNBD(id string, name string)                              {}

func (m *Metrics) AddWaitingCache(id string, name string, wc *waitingcache.Remote) {}
func (m *Metrics) RemoveWaitingCache(id string, name string)                       {}

func (m *Metrics) AddCopyOnWrite(id string, name string, cow *modules.CopyOnWrite) {}
func (m *Metrics) RemoveCopyOnWrite(id string, name string)                        {}
