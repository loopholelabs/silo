package statsd

import (
	"context"
	"fmt"
	"sync"
	"time"

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

type MetricsConfig struct {
	HeatmapResolution     uint64
	Namespace             string
	SubSyncer             string
	SubMigrator           string
	SubProtocol           string
	SubToProtocol         string
	SubFromProtocol       string
	SubS3                 string
	SubDirtyTracker       string
	SubVolatilityMonitor  string
	SubMetrics            string
	SubNBD                string
	SubWaitingCache       string
	SubCopyOnWrite        string
	TickMigrator          time.Duration
	TickSyncer            time.Duration
	TickProtocol          time.Duration
	TickToProtocol        time.Duration
	TickFromProtocol      time.Duration
	TickS3                time.Duration
	TickDirtyTracker      time.Duration
	TickVolatilityMonitor time.Duration
	TickMetrics           time.Duration
	TickNBD               time.Duration
	TickWaitingCache      time.Duration
	TickCopyOnWrite       time.Duration
}

func DefaultConfig() *MetricsConfig {
	return &MetricsConfig{
		HeatmapResolution:     64,
		Namespace:             "silo",
		SubSyncer:             "syncer",
		SubMigrator:           "migrator",
		SubProtocol:           "protocol",
		SubToProtocol:         "toProtocol",
		SubFromProtocol:       "fromProtocol",
		SubS3:                 "s3",
		SubDirtyTracker:       "dirtyTracker",
		SubVolatilityMonitor:  "volatilityMonitor",
		SubMetrics:            "metrics",
		SubNBD:                "nbd",
		SubWaitingCache:       "waitingCache",
		SubCopyOnWrite:        "cow",
		TickMigrator:          100 * time.Millisecond,
		TickSyncer:            100 * time.Millisecond,
		TickProtocol:          100 * time.Millisecond,
		TickToProtocol:        100 * time.Millisecond,
		TickFromProtocol:      100 * time.Millisecond,
		TickS3:                100 * time.Millisecond,
		TickDirtyTracker:      100 * time.Millisecond,
		TickVolatilityMonitor: 100 * time.Millisecond,
		TickMetrics:           100 * time.Millisecond,
		TickNBD:               100 * time.Millisecond,
		TickWaitingCache:      100 * time.Millisecond,
		TickCopyOnWrite:       100 * time.Millisecond,
	}
}

type Metrics struct {
	config    *MetricsConfig
	client    *statsd.Client
	lock      sync.Mutex
	cancelfns map[string]map[string]context.CancelFunc
}

func NewMetrics(config *MetricsConfig) *Metrics {
	client := statsd.NewClient("localhost:8125",
		statsd.MaxPacketSize(1400),
		statsd.MetricPrefix("silo."))

	return &Metrics{
		config:    config,
		client:    client,
		cancelfns: make(map[string]map[string]context.CancelFunc),
	}
}

func (m *Metrics) remove(subsystem string, id string, name string) {
	m.lock.Lock()
	cancelfns, ok := m.cancelfns[id]
	if ok {
		cancelfn, ok := cancelfns[fmt.Sprintf("%s_%s", subsystem, name)]
		if ok {
			cancelfn()
			delete(cancelfns, fmt.Sprintf("%s_%s", subsystem, name))
			if len(cancelfns) == 0 {
				delete(m.cancelfns, id)
			}
		}
	}
	m.lock.Unlock()
}

func (m *Metrics) add(subsystem string, id string, name string, interval time.Duration, tickfn func()) {
	m.lock.Lock()
	cancelfns, ok := m.cancelfns[id]
	if !ok {
		cancelfns = make(map[string]context.CancelFunc)
		m.cancelfns[id] = cancelfns
	}
	_, existing := cancelfns[fmt.Sprintf("%s_%s", subsystem, name)]
	if existing {
		// The metric is already being tracked.
		m.lock.Unlock()
		return
	}

	ctx, cancelfn := context.WithCancel(context.TODO())
	cancelfns[fmt.Sprintf("%s_%s", subsystem, name)] = cancelfn

	m.lock.Unlock()

	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tickfn()
			}
		}
	}()
}

func (m *Metrics) Shutdown() {
	m.lock.Lock()
	for _, cancelfns := range m.cancelfns {
		for _, cancelfn := range cancelfns {
			cancelfn()
		}
	}
	m.cancelfns = make(map[string]map[string]context.CancelFunc)
	m.lock.Unlock()
}

func (m *Metrics) RemoveAllID(id string) {
	m.lock.Lock()
	cancelfns, ok := m.cancelfns[id]
	if ok {
		for _, cancelfn := range cancelfns {
			cancelfn()
		}
		delete(m.cancelfns, id)
	}
	m.lock.Unlock()
}

func (m *Metrics) AddSyncer(id string, name string, sync *migrator.Syncer) {}
func (m *Metrics) RemoveSyncer(id string, name string) {
	m.remove(m.config.SubSyncer, id, name)
}

func (m *Metrics) AddMigrator(id string, name string, mig *migrator.Migrator) {}
func (m *Metrics) RemoveMigrator(id string, name string) {
	m.remove(m.config.SubMigrator, id, name)
}

func (m *Metrics) AddProtocol(id string, name string, proto *protocol.RW) {}
func (m *Metrics) RemoveProtocol(id string, name string) {
	m.remove(m.config.SubProtocol, id, name)
}

func (m *Metrics) AddToProtocol(id string, name string, proto *protocol.ToProtocol) {}
func (m *Metrics) RemoveToProtocol(id string, name string) {
	m.remove(m.config.SubToProtocol, id, name)
}

func (m *Metrics) AddFromProtocol(id string, name string, proto *protocol.FromProtocol) {}
func (m *Metrics) RemoveFromProtocol(id string, name string) {
	m.remove(m.config.SubFromProtocol, id, name)
}

func (m *Metrics) AddS3Storage(id string, name string, s3 *sources.S3Storage) {}
func (m *Metrics) RemoveS3Storage(id string, name string) {
	m.remove(m.config.SubS3, id, name)
}

func (m *Metrics) AddDirtyTracker(id string, name string, dt *dirtytracker.Remote) {}
func (m *Metrics) RemoveDirtyTracker(id string, name string) {
	m.remove(m.config.SubDirtyTracker, id, name)
}

func (m *Metrics) AddVolatilityMonitor(id string, name string, vm *volatilitymonitor.VolatilityMonitor) {
}
func (m *Metrics) RemoveVolatilityMonitor(id string, name string) {
	m.remove(m.config.SubVolatilityMonitor, id, name)
}

func (m *Metrics) AddMetrics(id string, name string, mm *modules.Metrics) {}
func (m *Metrics) RemoveMetrics(id string, name string) {
	m.remove(m.config.SubMetrics, id, name)
}

func (m *Metrics) AddNBD(id string, name string, mm *expose.ExposedStorageNBDNL) {}
func (m *Metrics) RemoveNBD(id string, name string) {
	m.remove(m.config.SubNBD, id, name)
}

func (m *Metrics) AddWaitingCache(id string, name string, wc *waitingcache.Remote) {}
func (m *Metrics) RemoveWaitingCache(id string, name string) {
	m.remove(m.config.SubWaitingCache, id, name)
}

func (m *Metrics) AddCopyOnWrite(id string, name string, cow *modules.CopyOnWrite) {}
func (m *Metrics) RemoveCopyOnWrite(id string, name string) {
	m.remove(m.config.SubCopyOnWrite, id, name)
}
