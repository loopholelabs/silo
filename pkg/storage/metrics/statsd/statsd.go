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

func New(addr string, config *MetricsConfig) *Metrics {
	client := statsd.NewClient(addr,
		statsd.MaxPacketSize(1400),
		statsd.TagStyle(statsd.TagFormatDatadog),
		statsd.MetricPrefix("silo."))

	return &Metrics{
		config:    config,
		client:    client,
		cancelfns: make(map[string]map[string]context.CancelFunc),
	}
}

func (m *Metrics) remove(subsystem string, id string, name string) {
	m.lock.Lock()
	defer m.lock.Unlock()
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

func (m *Metrics) updateMetric(id string, name string, sub string, metricName string, v1 uint64, v2 uint64) {
	if v1 != v2 {
		m.client.Gauge(fmt.Sprintf("%s_%s", sub, metricName), int64(v2), statsd.StringTag("id", id), statsd.StringTag("name", name))
	}
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

func (m *Metrics) AddSyncer(id string, name string, syncer *migrator.Syncer) {
	lastmet := &migrator.MigrationProgress{}
	m.add(m.config.SubSyncer, id, name, m.config.TickSyncer, func() {
		met := syncer.GetMetrics()
		if met != nil {
			m.updateMetric(id, name, m.config.SubSyncer, "block_size", uint64(lastmet.BlockSize), uint64(met.BlockSize))
			m.updateMetric(id, name, m.config.SubSyncer, "total_blocks", uint64(lastmet.TotalBlocks), uint64(met.TotalBlocks))
			m.updateMetric(id, name, m.config.SubSyncer, "active_blocks", uint64(lastmet.ActiveBlocks), uint64(met.ActiveBlocks))
			m.updateMetric(id, name, m.config.SubSyncer, "migrated_blocks", uint64(lastmet.MigratedBlocks), uint64(met.MigratedBlocks))
			m.updateMetric(id, name, m.config.SubSyncer, "ready_blocks", uint64(lastmet.ReadyBlocks), uint64(met.ReadyBlocks))
			m.updateMetric(id, name, m.config.SubSyncer, "total_migrated_blocks", uint64(lastmet.TotalMigratedBlocks), uint64(met.TotalMigratedBlocks))
		}
		lastmet = met
	})
}

func (m *Metrics) RemoveSyncer(id string, name string) {
	m.remove(m.config.SubSyncer, id, name)
}

func (m *Metrics) AddMigrator(id string, name string, mig *migrator.Migrator) {
	lastmet := &migrator.MigrationProgress{}
	m.add(m.config.SubSyncer, id, name, m.config.TickSyncer, func() {
		met := mig.GetMetrics()
		if met != nil {
			m.updateMetric(id, name, m.config.SubSyncer, "block_size", uint64(lastmet.BlockSize), uint64(met.BlockSize))
			m.updateMetric(id, name, m.config.SubSyncer, "total_blocks", uint64(lastmet.TotalBlocks), uint64(met.TotalBlocks))
			m.updateMetric(id, name, m.config.SubSyncer, "active_blocks", uint64(lastmet.ActiveBlocks), uint64(met.ActiveBlocks))
			m.updateMetric(id, name, m.config.SubSyncer, "migrated_blocks", uint64(lastmet.MigratedBlocks), uint64(met.MigratedBlocks))
			m.updateMetric(id, name, m.config.SubSyncer, "ready_blocks", uint64(lastmet.ReadyBlocks), uint64(met.ReadyBlocks))
			m.updateMetric(id, name, m.config.SubSyncer, "total_migrated_blocks", uint64(lastmet.TotalMigratedBlocks), uint64(met.TotalMigratedBlocks))
		}
		lastmet = met
	})
}
func (m *Metrics) RemoveMigrator(id string, name string) {
	m.remove(m.config.SubMigrator, id, name)
}

func (m *Metrics) AddProtocol(id string, name string, proto *protocol.RW) {
	lastmet := &protocol.Metrics{}
	m.add(m.config.SubProtocol, id, name, m.config.TickProtocol, func() {
		met := proto.GetMetrics()
		m.updateMetric(id, name, m.config.SubProtocol, "active_packets_sending", lastmet.ActivePacketsSending, met.ActivePacketsSending)
		m.updateMetric(id, name, m.config.SubProtocol, "packets_sent", lastmet.PacketsSent, met.PacketsSent)
		m.updateMetric(id, name, m.config.SubProtocol, "data_sent", lastmet.DataSent, met.DataSent)
		m.updateMetric(id, name, m.config.SubProtocol, "packets_recv", lastmet.PacketsRecv, met.PacketsRecv)
		m.updateMetric(id, name, m.config.SubProtocol, "data_recv", lastmet.DataRecv, met.DataRecv)
		m.updateMetric(id, name, m.config.SubProtocol, "writes", lastmet.Writes, met.Writes)
		m.updateMetric(id, name, m.config.SubProtocol, "write_errors", lastmet.WriteErrors, met.WriteErrors)
		m.updateMetric(id, name, m.config.SubProtocol, "waiting_for_id", uint64(lastmet.WaitingForID), uint64(met.WaitingForID))
		lastmet = met
	})
}
func (m *Metrics) RemoveProtocol(id string, name string) {
	m.remove(m.config.SubProtocol, id, name)
}

func (m *Metrics) AddToProtocol(id string, name string, proto *protocol.ToProtocol) {
	lastmet := &protocol.ToProtocolMetrics{}
	m.add(m.config.SubToProtocol, id, name, m.config.TickToProtocol, func() {
		met := proto.GetMetrics()
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_events", lastmet.SentEvents, met.SentEvents)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_alt_sources", lastmet.SentAltSources, met.SentAltSources)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_hashes", lastmet.SentHashes, met.SentHashes)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_dev_info", lastmet.SentDevInfo, met.SentDevInfo)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_dirty_list", lastmet.SentDirtyList, met.SentDirtyList)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_read_at", lastmet.SentReadAt, met.SentReadAt)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_write_at_hash", lastmet.SentWriteAtHash, met.SentWriteAtHash)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_write_at_hash_bytes", lastmet.SentWriteAtHashBytes, met.SentWriteAtHashBytes)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_write_at_comp", lastmet.SentWriteAtComp, met.SentWriteAtComp)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_write_at_comp_bytes", lastmet.SentWriteAtCompBytes, met.SentWriteAtCompBytes)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_write_at_comp_data_bytes", lastmet.SentWriteAtCompDataBytes, met.SentWriteAtCompDataBytes)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_write_at", lastmet.SentWriteAt, met.SentWriteAt)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_write_at_bytes", lastmet.SentWriteAtBytes, met.SentWriteAtBytes)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_write_at_with_map", lastmet.SentWriteAtWithMap, met.SentWriteAtWithMap)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_remove_from_map", lastmet.SentRemoveFromMap, met.SentRemoveFromMap)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_you_already_have", lastmet.SentYouAlreadyHave, met.SentYouAlreadyHave)
		m.updateMetric(id, name, m.config.SubToProtocol, "sent_you_already_have_bytes", lastmet.SentYouAlreadyHaveBytes, met.SentYouAlreadyHaveBytes)
		m.updateMetric(id, name, m.config.SubToProtocol, "recv_need_at", lastmet.RecvNeedAt, met.RecvNeedAt)
		m.updateMetric(id, name, m.config.SubToProtocol, "recv_dont_need_at", lastmet.RecvDontNeedAt, met.RecvDontNeedAt)
		lastmet = met
	})
}
func (m *Metrics) RemoveToProtocol(id string, name string) {
	m.remove(m.config.SubToProtocol, id, name)
}

func (m *Metrics) AddFromProtocol(id string, name string, proto *protocol.FromProtocol) {
	lastmet := &protocol.FromProtocolMetrics{
		AvailableP2P:        make([]uint, 0),
		DuplicateP2P:        make([]uint, 0),
		AvailableAltSources: make([]uint, 0),
	}
	m.add(m.config.SubFromProtocol, id, name, m.config.TickFromProtocol, func() {
		met := proto.GetMetrics()

		if met.DeviceName != "" {
			name = met.DeviceName
		}

		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_events", lastmet.RecvEvents, met.RecvEvents)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_hashes", lastmet.RecvHashes, met.RecvHashes)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_dev_info", lastmet.RecvDevInfo, met.RecvDevInfo)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_alt_sources", lastmet.RecvAltSources, met.RecvAltSources)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_read_at", lastmet.RecvReadAt, met.RecvReadAt)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_write_at_hash", lastmet.RecvWriteAtHash, met.RecvWriteAtHash)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_write_at_comp", lastmet.RecvWriteAtComp, met.RecvWriteAtComp)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_write_at", lastmet.RecvWriteAt, met.RecvWriteAt)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_write_at_with_map", lastmet.RecvWriteAtWithMap, met.RecvWriteAtWithMap)

		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_remove_from_map", lastmet.RecvRemoveFromMap, met.RecvRemoveFromMap)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_remove_dev", lastmet.RecvRemoveDev, met.RecvRemoveDev)
		m.updateMetric(id, name, m.config.SubFromProtocol, "recv_dirty_list", lastmet.RecvDirtyList, met.RecvDirtyList)
		m.updateMetric(id, name, m.config.SubFromProtocol, "sent_need_at", lastmet.SentNeedAt, met.SentNeedAt)
		m.updateMetric(id, name, m.config.SubFromProtocol, "sent_dont_need_at", lastmet.SentDontNeedAt, met.SentDontNeedAt)

		m.updateMetric(id, name, m.config.SubFromProtocol, "writes_allowed_p2p", lastmet.WritesAllowedP2P, met.WritesAllowedP2P)
		m.updateMetric(id, name, m.config.SubFromProtocol, "writes_blocked_p2p", lastmet.WritesBlockedP2P, met.WritesBlockedP2P)
		m.updateMetric(id, name, m.config.SubFromProtocol, "writes_allowed_alt_sources", lastmet.WritesAllowedAltSources, met.WritesAllowedAltSources)
		m.updateMetric(id, name, m.config.SubFromProtocol, "writes_blocked_alt_sources", lastmet.WritesBlockedAltSources, met.WritesBlockedAltSources)

		totalHeatmapP2P := make([]uint64, m.config.HeatmapResolution)
		for _, block := range met.AvailableP2P {
			part := uint64(block) * m.config.HeatmapResolution / met.NumBlocks
			totalHeatmapP2P[part]++
		}

		lastTotalHeatmapP2P := make([]uint64, m.config.HeatmapResolution)
		for _, block := range lastmet.AvailableP2P {
			part := uint64(block) * m.config.HeatmapResolution / lastmet.NumBlocks
			lastTotalHeatmapP2P[part]++
		}

		totalHeatmapAltSources := make([]uint64, m.config.HeatmapResolution)
		for _, block := range met.AvailableAltSources {
			part := uint64(block) * m.config.HeatmapResolution / met.NumBlocks
			totalHeatmapAltSources[part]++
		}

		lastTotalHeatmapAltSources := make([]uint64, m.config.HeatmapResolution)
		for _, block := range lastmet.AvailableAltSources {
			part := uint64(block) * m.config.HeatmapResolution / lastmet.NumBlocks
			lastTotalHeatmapAltSources[part]++
		}

		totalHeatmapP2PDupe := make([]uint64, m.config.HeatmapResolution)
		for _, block := range met.DuplicateP2P {
			part := uint64(block) * m.config.HeatmapResolution / met.NumBlocks
			totalHeatmapP2PDupe[part]++
		}

		lastTotalHeatmapP2PDupe := make([]uint64, m.config.HeatmapResolution)
		for _, block := range lastmet.DuplicateP2P {
			part := uint64(block) * m.config.HeatmapResolution / lastmet.NumBlocks
			lastTotalHeatmapP2PDupe[part]++
		}

		for part, blocks := range totalHeatmapP2P {
			m.updateMetric(id, name, m.config.SubFromProtocol, fmt.Sprintf("heatmap_p2p_%d", part), lastTotalHeatmapP2P[part], blocks)
		}

		for part, blocks := range totalHeatmapAltSources {
			if blocks > 0 {
				m.updateMetric(id, name, m.config.SubFromProtocol, fmt.Sprintf("heatmap_alt_sources_%d", part), lastTotalHeatmapAltSources[part], blocks)
			}
		}

		for part, blocks := range totalHeatmapP2PDupe {
			if blocks > 0 {
				m.updateMetric(id, name, m.config.SubFromProtocol, fmt.Sprintf("heatmap_p2p_dupe_%d", part), lastTotalHeatmapP2PDupe[part], blocks)
			}
		}

		lastmet = met
	})

}
func (m *Metrics) RemoveFromProtocol(id string, name string) {
	m.remove(m.config.SubFromProtocol, id, name)
}

func (m *Metrics) AddS3Storage(id string, name string, s3 *sources.S3Storage) {
	lastmet := &sources.S3Metrics{}
	m.add(m.config.SubS3, id, name, m.config.TickS3, func() {
		met := s3.Metrics()
		m.updateMetric(id, name, m.config.SubS3, "blocks_w", lastmet.BlocksWCount, met.BlocksWCount)
		m.updateMetric(id, name, m.config.SubS3, "blocks_w_bytes", lastmet.BlocksWBytes, met.BlocksWBytes)
		m.updateMetric(id, name, m.config.SubS3, "blocks_r", lastmet.BlocksRCount, met.BlocksRCount)
		m.updateMetric(id, name, m.config.SubS3, "blocks_r_bytes", lastmet.BlocksRBytes, met.BlocksRBytes)
		m.updateMetric(id, name, m.config.SubS3, "active_reads", lastmet.ActiveReads, met.ActiveReads)
		m.updateMetric(id, name, m.config.SubS3, "active_writes", lastmet.ActiveWrites, met.ActiveWrites)
		lastmet = met
	})

}
func (m *Metrics) RemoveS3Storage(id string, name string) {
	m.remove(m.config.SubS3, id, name)
}

func (m *Metrics) AddDirtyTracker(id string, name string, dt *dirtytracker.Remote) {
	lastmet := &dirtytracker.Metrics{}
	m.add(m.config.SubDirtyTracker, id, name, m.config.TickDirtyTracker, func() {
		met := dt.GetMetrics()
		m.updateMetric(id, name, m.config.SubDirtyTracker, "block_size", lastmet.BlockSize, met.BlockSize)
		m.updateMetric(id, name, m.config.SubDirtyTracker, "tracking_blocks", lastmet.TrackingBlocks, met.TrackingBlocks)
		m.updateMetric(id, name, m.config.SubDirtyTracker, "dirty_blocks", lastmet.DirtyBlocks, met.DirtyBlocks)
		m.updateMetric(id, name, m.config.SubDirtyTracker, "block_max_age", uint64(lastmet.MaxAgeDirty), uint64(met.MaxAgeDirty))
		lastmet = met
	})
}
func (m *Metrics) RemoveDirtyTracker(id string, name string) {
	m.remove(m.config.SubDirtyTracker, id, name)
}

func (m *Metrics) AddVolatilityMonitor(id string, name string, vm *volatilitymonitor.VolatilityMonitor) {
	lastmet := &volatilitymonitor.Metrics{
		VolatilityMap: make(map[int]uint64),
	}
	m.add(m.config.SubVolatilityMonitor, id, name, m.config.TickVolatilityMonitor, func() {
		met := vm.GetMetrics()
		m.updateMetric(id, name, m.config.SubVolatilityMonitor, "block_size", lastmet.BlockSize, met.BlockSize)
		m.updateMetric(id, name, m.config.SubVolatilityMonitor, "available", lastmet.Available, met.Available)
		m.updateMetric(id, name, m.config.SubVolatilityMonitor, "volatility", lastmet.Volatility, met.Volatility)

		totalVolatility := make([]uint64, m.config.HeatmapResolution)
		lastTotalVolatility := make([]uint64, m.config.HeatmapResolution)
		for block, volatility := range met.VolatilityMap {
			part := uint64(block) * m.config.HeatmapResolution / met.NumBlocks
			totalVolatility[part] += volatility

			lastVolatility := lastmet.VolatilityMap[block]
			lastTotalVolatility[part] += lastVolatility
		}

		for part, volatility := range totalVolatility {
			lastVolatility := lastTotalVolatility[part]
			m.updateMetric(id, name, m.config.SubVolatilityMonitor, fmt.Sprintf("volatility_%d", part), lastVolatility, volatility)
		}
		lastmet = met
	})
}

func (m *Metrics) RemoveVolatilityMonitor(id string, name string) {
	m.remove(m.config.SubVolatilityMonitor, id, name)
}

func (m *Metrics) AddMetrics(id string, name string, mm *modules.Metrics) {
	lastmet := &modules.MetricsSnapshot{
		ReadOpsSize:  make(map[int]uint64),
		WriteOpsSize: make(map[int]uint64),
	}

	m.add(m.config.SubMetrics, id, name, m.config.TickMetrics, func() {
		met := mm.GetMetrics()
		m.updateMetric(id, name, m.config.SubMetrics, "read_ops", lastmet.ReadOps, met.ReadOps)
		m.updateMetric(id, name, m.config.SubMetrics, "read_bytes", lastmet.ReadBytes, met.ReadBytes)
		m.updateMetric(id, name, m.config.SubMetrics, "read_errors", lastmet.ReadErrors, met.ReadErrors)
		m.updateMetric(id, name, m.config.SubMetrics, "read_time", lastmet.ReadTime, met.ReadTime)
		m.updateMetric(id, name, m.config.SubMetrics, "write_ops", lastmet.WriteOps, met.WriteOps)
		m.updateMetric(id, name, m.config.SubMetrics, "write_bytes", lastmet.WriteBytes, met.WriteBytes)
		m.updateMetric(id, name, m.config.SubMetrics, "write_errors", lastmet.WriteErrors, met.WriteErrors)
		m.updateMetric(id, name, m.config.SubMetrics, "write_time", lastmet.WriteTime, met.WriteTime)
		m.updateMetric(id, name, m.config.SubMetrics, "flush_ops", lastmet.FlushOps, met.FlushOps)
		m.updateMetric(id, name, m.config.SubMetrics, "flush_errors", lastmet.FlushErrors, met.FlushErrors)
		m.updateMetric(id, name, m.config.SubMetrics, "flush_time", lastmet.FlushTime, met.FlushTime)

		// Add the size metrics here...
		for _, v := range modules.OpSizeBuckets {
			m.updateMetric(id, name, m.config.SubMetrics, fmt.Sprintf("read_ops_size_%d", v), lastmet.ReadOpsSize[v], met.ReadOpsSize[v])
			m.updateMetric(id, name, m.config.SubMetrics, fmt.Sprintf("write_ops_size_%d", v), lastmet.WriteOpsSize[v], met.WriteOpsSize[v])
		}
		lastmet = met
	})
}

func (m *Metrics) RemoveMetrics(id string, name string) {
	m.remove(m.config.SubMetrics, id, name)
}

func (m *Metrics) AddNBD(id string, name string, mm *expose.ExposedStorageNBDNL) {
	lastmet := &expose.DispatchMetrics{}

	m.add(m.config.SubNBD, id, name, m.config.TickNBD, func() {
		met := mm.GetMetrics()
		m.updateMetric(id, name, m.config.SubNBD, "packets_in", lastmet.PacketsIn, met.PacketsIn)
		m.updateMetric(id, name, m.config.SubNBD, "packets_out", lastmet.PacketsOut, met.PacketsOut)
		m.updateMetric(id, name, m.config.SubNBD, "read_at", lastmet.ReadAt, met.ReadAt)
		m.updateMetric(id, name, m.config.SubNBD, "read_at_bytes", lastmet.ReadAtBytes, met.ReadAtBytes)
		m.updateMetric(id, name, m.config.SubNBD, "read_at_time", uint64(lastmet.ReadAtTime), uint64(met.ReadAtTime))
		m.updateMetric(id, name, m.config.SubNBD, "active_reads", lastmet.ActiveReads, met.ActiveReads)
		m.updateMetric(id, name, m.config.SubNBD, "write_at", lastmet.WriteAt, met.WriteAt)
		m.updateMetric(id, name, m.config.SubNBD, "write_at_bytes", lastmet.WriteAtBytes, met.WriteAtBytes)
		m.updateMetric(id, name, m.config.SubNBD, "write_at_time", uint64(lastmet.WriteAtTime), uint64(met.WriteAtTime))
		m.updateMetric(id, name, m.config.SubNBD, "active_writes", lastmet.ActiveWrites, met.ActiveWrites)
		lastmet = met
	})
}
func (m *Metrics) RemoveNBD(id string, name string) {
	m.remove(m.config.SubNBD, id, name)
}

func (m *Metrics) AddWaitingCache(id string, name string, wc *waitingcache.Remote) {
	lastmet := &waitingcache.Metrics{}
	m.add(m.config.SubWaitingCache, id, name, m.config.TickWaitingCache, func() {
		met := wc.GetMetrics()
		m.updateMetric(id, name, m.config.SubWaitingCache, "waiting_for_block", lastmet.WaitForBlock, met.WaitForBlock)
		m.updateMetric(id, name, m.config.SubWaitingCache, "waiting_for_block_had_remote", lastmet.WaitForBlockHadRemote, met.WaitForBlockHadRemote)
		m.updateMetric(id, name, m.config.SubWaitingCache, "waiting_for_block_had_local", lastmet.WaitForBlockHadLocal, met.WaitForBlockHadLocal)
		m.updateMetric(id, name, m.config.SubWaitingCache, "waiting_for_block_lock", lastmet.WaitForBlockLock, met.WaitForBlockLock)
		m.updateMetric(id, name, m.config.SubWaitingCache, "waiting_for_block_lock_done", lastmet.WaitForBlockLockDone, met.WaitForBlockLockDone)
		m.updateMetric(id, name, m.config.SubWaitingCache, "mark_available_local_block", lastmet.MarkAvailableLocalBlock, met.MarkAvailableLocalBlock)
		m.updateMetric(id, name, m.config.SubWaitingCache, "mark_available_remote_block", lastmet.MarkAvailableRemoteBlock, met.MarkAvailableRemoteBlock)
		m.updateMetric(id, name, m.config.SubWaitingCache, "available_local", lastmet.AvailableLocal, met.AvailableLocal)
		m.updateMetric(id, name, m.config.SubWaitingCache, "available_remote", lastmet.AvailableRemote, met.AvailableRemote)
		lastmet = met
	})

}
func (m *Metrics) RemoveWaitingCache(id string, name string) {
	m.remove(m.config.SubWaitingCache, id, name)
}

func (m *Metrics) AddCopyOnWrite(id string, name string, cow *modules.CopyOnWrite) {
	lastmet := &modules.CopyOnWriteMetrics{}
	m.add(m.config.SubCopyOnWrite, id, name, m.config.TickCopyOnWrite, func() {
		met := cow.GetMetrics()
		m.updateMetric(id, name, m.config.SubCopyOnWrite, "size", lastmet.MetricSize, met.MetricSize)
		m.updateMetric(id, name, m.config.SubCopyOnWrite, "nonzero_size", lastmet.MetricNonZeroSize, met.MetricNonZeroSize)
		m.updateMetric(id, name, m.config.SubCopyOnWrite, "overlay_size", lastmet.MetricOverlaySize, met.MetricOverlaySize)
		m.updateMetric(id, name, m.config.SubCopyOnWrite, "zero_read_ops", lastmet.MetricZeroReadOps, met.MetricZeroReadOps)
		m.updateMetric(id, name, m.config.SubCopyOnWrite, "zero_read_bytes", lastmet.MetricZeroReadBytes, met.MetricZeroReadBytes)
		m.updateMetric(id, name, m.config.SubCopyOnWrite, "zero_pre_write_read_ops", lastmet.MetricZeroPreWriteReadOps, met.MetricZeroPreWriteReadOps)
		m.updateMetric(id, name, m.config.SubCopyOnWrite, "zero_pre_write_read_bytes", lastmet.MetricZeroPreWriteReadBytes, met.MetricZeroPreWriteReadBytes)
		lastmet = met
	})
}

func (m *Metrics) RemoveCopyOnWrite(id string, name string) {
	m.remove(m.config.SubCopyOnWrite, id, name)
}
