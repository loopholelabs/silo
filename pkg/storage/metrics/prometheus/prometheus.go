package prometheus

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
	"github.com/prometheus/client_golang/prometheus"
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
	}
}

type Metrics struct {
	reg    prometheus.Registerer
	lock   sync.Mutex
	config *MetricsConfig

	// syncer
	syncerBlockSize           *prometheus.GaugeVec
	syncerTotalBlocks         *prometheus.GaugeVec
	syncerMigratedBlocks      *prometheus.GaugeVec
	syncerReadyBlocks         *prometheus.GaugeVec
	syncerActiveBlocks        *prometheus.GaugeVec
	syncerTotalMigratedBlocks *prometheus.GaugeVec

	// migrator
	migratorBlockSize           *prometheus.GaugeVec
	migratorTotalBlocks         *prometheus.GaugeVec
	migratorMigratedBlocks      *prometheus.GaugeVec
	migratorReadyBlocks         *prometheus.GaugeVec
	migratorActiveBlocks        *prometheus.GaugeVec
	migratorTotalMigratedBlocks *prometheus.GaugeVec

	// protocol
	protocolActivePacketsSending *prometheus.GaugeVec
	protocolPacketsSent          *prometheus.GaugeVec
	protocolDataSent             *prometheus.GaugeVec
	protocolPacketsRecv          *prometheus.GaugeVec
	protocolDataRecv             *prometheus.GaugeVec
	protocolWrites               *prometheus.GaugeVec
	protocolWriteErrors          *prometheus.GaugeVec
	protocolWaitingForID         *prometheus.GaugeVec

	// s3
	s3BlocksR      *prometheus.GaugeVec
	s3BlocksRBytes *prometheus.GaugeVec
	s3BlocksW      *prometheus.GaugeVec
	s3BlocksWBytes *prometheus.GaugeVec
	s3ActiveReads  *prometheus.GaugeVec
	s3ActiveWrites *prometheus.GaugeVec

	// toProtocol
	toProtocolSentEvents               *prometheus.GaugeVec
	toProtocolSentAltSources           *prometheus.GaugeVec
	toProtocolSentHashes               *prometheus.GaugeVec
	toProtocolSentDevInfo              *prometheus.GaugeVec
	toProtocolSentDirtyList            *prometheus.GaugeVec
	toProtocolSentReadAt               *prometheus.GaugeVec
	toProtocolSentWriteAtHash          *prometheus.GaugeVec
	toProtocolSentWriteAtHashBytes     *prometheus.GaugeVec
	toProtocolSentWriteAtComp          *prometheus.GaugeVec
	toProtocolSentWriteAtCompBytes     *prometheus.GaugeVec
	toProtocolSentWriteAtCompDataBytes *prometheus.GaugeVec
	toProtocolSentWriteAt              *prometheus.GaugeVec
	toProtocolSentWriteAtBytes         *prometheus.GaugeVec
	toProtocolSentWriteAtWithMap       *prometheus.GaugeVec
	toProtocolSentRemoveFromMap        *prometheus.GaugeVec
	toProtocolSentYouAlreadyHave       *prometheus.GaugeVec
	toProtocolSentYouAlreadyHaveBytes  *prometheus.GaugeVec
	toProtocolRecvNeedAt               *prometheus.GaugeVec
	toProtocolRecvDontNeedAt           *prometheus.GaugeVec

	// fromProtocol
	fromProtocolRecvEvents              *prometheus.GaugeVec
	fromProtocolRecvHashes              *prometheus.GaugeVec
	fromProtocolRecvDevInfo             *prometheus.GaugeVec
	fromProtocolRecvAltSources          *prometheus.GaugeVec
	fromProtocolRecvReadAt              *prometheus.GaugeVec
	fromProtocolRecvWriteAtHash         *prometheus.GaugeVec
	fromProtocolRecvWriteAtComp         *prometheus.GaugeVec
	fromProtocolRecvWriteAt             *prometheus.GaugeVec
	fromProtocolRecvWriteAtWithMap      *prometheus.GaugeVec
	fromProtocolRecvRemoveFromMap       *prometheus.GaugeVec
	fromProtocolRecvRemoveDev           *prometheus.GaugeVec
	fromProtocolRecvDirtyList           *prometheus.GaugeVec
	fromProtocolSentNeedAt              *prometheus.GaugeVec
	fromProtocolSentDontNeedAt          *prometheus.GaugeVec
	fromProtocolWritesAllowedP2P        *prometheus.GaugeVec
	fromProtocolWritesBlockedP2P        *prometheus.GaugeVec
	fromProtocolWritesAllowedAltSources *prometheus.GaugeVec
	fromProtocolWritesBlockedAltSources *prometheus.GaugeVec
	fromProtocolHeatmap                 *prometheus.GaugeVec

	// dirtyTracker
	dirtyTrackerBlockSize      *prometheus.GaugeVec
	dirtyTrackerTrackingBlocks *prometheus.GaugeVec
	dirtyTrackerDirtyBlocks    *prometheus.GaugeVec
	dirtyTrackerMaxAgeDirtyMS  *prometheus.GaugeVec

	// volatilityMonitor
	volatilityMonitorBlockSize  *prometheus.GaugeVec
	volatilityMonitorAvailable  *prometheus.GaugeVec
	volatilityMonitorVolatility *prometheus.GaugeVec
	volatilityMonitorHeatmap    *prometheus.GaugeVec

	// metrics
	metricsReadOps     *prometheus.GaugeVec
	metricsReadBytes   *prometheus.GaugeVec
	metricsReadTime    *prometheus.GaugeVec
	metricsReadErrors  *prometheus.GaugeVec
	metricsWriteOps    *prometheus.GaugeVec
	metricsWriteBytes  *prometheus.GaugeVec
	metricsWriteTime   *prometheus.GaugeVec
	metricsWriteErrors *prometheus.GaugeVec
	metricsFlushOps    *prometheus.GaugeVec
	metricsFlushTime   *prometheus.GaugeVec
	metricsFlushErrors *prometheus.GaugeVec

	// nbd
	nbdPacketsIn    *prometheus.GaugeVec
	nbdPacketsOut   *prometheus.GaugeVec
	nbdReadAt       *prometheus.GaugeVec
	nbdReadAtBytes  *prometheus.GaugeVec
	nbdWriteAt      *prometheus.GaugeVec
	nbdWriteAtBytes *prometheus.GaugeVec

	// waitingCache
	waitingCacheWaitForBlock             *prometheus.GaugeVec
	waitingCacheWaitForBlockHadRemote    *prometheus.GaugeVec
	waitingCacheWaitForBlockHadLocal     *prometheus.GaugeVec
	waitingCacheWaitForBlockLock         *prometheus.GaugeVec
	waitingCacheWaitForBlockLockDone     *prometheus.GaugeVec
	waitingCacheMarkAvailableLocalBlock  *prometheus.GaugeVec
	waitingCacheMarkAvailableRemoteBlock *prometheus.GaugeVec
	waitingCacheAvailableLocal           *prometheus.GaugeVec
	waitingCacheAvailableRemote          *prometheus.GaugeVec

	cancelfns map[string]context.CancelFunc
}

func New(reg prometheus.Registerer, config *MetricsConfig) *Metrics {

	labels := []string{"instance_id", "device"}

	met := &Metrics{
		config: config,
		reg:    reg,
		// Syncer
		syncerBlockSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubSyncer, Name: "block_size", Help: "Block size"}, labels),
		syncerTotalBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubSyncer, Name: "total_blocks", Help: "Total blocks"}, labels),
		syncerActiveBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubSyncer, Name: "active_blocks", Help: "Active blocks"}, labels),
		syncerMigratedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubSyncer, Name: "migrated_blocks", Help: "Migrated blocks"}, labels),
		syncerReadyBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubSyncer, Name: "ready_blocks", Help: "Ready blocks"}, labels),
		syncerTotalMigratedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubSyncer, Name: "total_migrated_blocks", Help: "Total migrated blocks"}, labels),

		// Migrator
		migratorBlockSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMigrator, Name: "block_size", Help: "Block size"}, labels),
		migratorTotalBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMigrator, Name: "total_blocks", Help: "Total blocks"}, labels),
		migratorActiveBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMigrator, Name: "active_blocks", Help: "Active blocks"}, labels),
		migratorMigratedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMigrator, Name: "migrated_blocks", Help: "Migrated blocks"}, labels),
		migratorReadyBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMigrator, Name: "ready_blocks", Help: "Ready blocks"}, labels),
		migratorTotalMigratedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMigrator, Name: "total_migrated_blocks", Help: "Total migrated blocks"}, labels),

		// Protocol
		protocolActivePacketsSending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubProtocol, Name: "active_packets_sending", Help: "Packets sending"}, labels),
		protocolPacketsSent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubProtocol, Name: "packets_sent", Help: "Packets sent"}, labels),
		protocolDataSent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubProtocol, Name: "data_sent", Help: "Data sent"}, labels),
		protocolPacketsRecv: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubProtocol, Name: "packets_recv", Help: "Packets recv"}, labels),
		protocolDataRecv: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubProtocol, Name: "data_recv", Help: "Data recv"}, labels),
		protocolWrites: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubProtocol, Name: "writes", Help: "Writes"}, labels),
		protocolWriteErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubProtocol, Name: "write_errors", Help: "Write errors"}, labels),
		protocolWaitingForID: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubProtocol, Name: "waiting_for_id", Help: "Waiting for ID"}, labels),

		// ToProtocol
		toProtocolSentEvents: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_events", Help: "sentEvents"}, labels),
		toProtocolSentAltSources: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_alt_sources", Help: "sentAltSources"}, labels),
		toProtocolSentHashes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_hashes", Help: "sentHashes"}, labels),
		toProtocolSentDevInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_dev_info", Help: "sentDevInfo"}, labels),
		toProtocolSentDirtyList: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_dirty_list", Help: "sentDirtyList"}, labels),
		toProtocolSentReadAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_read_at", Help: "sentReadAt"}, labels),
		toProtocolSentWriteAtHash: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_write_at_hash", Help: "sentWriteAtHash"}, labels),
		toProtocolSentWriteAtHashBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_write_at_hash_bytes", Help: "sentWriteAtHashBytes"}, labels),
		toProtocolSentWriteAtComp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_write_at_comp", Help: "sentWriteAtComp"}, labels),
		toProtocolSentWriteAtCompBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_write_at_comp_bytes", Help: "sentWriteAtCompBytes"}, labels),
		toProtocolSentWriteAtCompDataBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_write_at_comp_data_bytes", Help: "sentWriteAtCompDataBytes"}, labels),
		toProtocolSentWriteAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_write_at", Help: "sentWriteAt"}, labels),
		toProtocolSentWriteAtBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_write_at_bytes", Help: "sentWriteAtBytes"}, labels),
		toProtocolSentWriteAtWithMap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_write_at_with_map", Help: "sentWriteAtWithMap"}, labels),
		toProtocolSentRemoveFromMap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_remove_from_map", Help: "sentRemoveFromMap"}, labels),
		toProtocolSentYouAlreadyHave: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_you_already_have", Help: "sentYouAlreadyHave"}, labels),
		toProtocolSentYouAlreadyHaveBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "sent_you_already_have_bytes", Help: "sentYouAlreadyHaveBytes"}, labels),
		toProtocolRecvNeedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "recv_need_at", Help: "recvNeedAt"}, labels),
		toProtocolRecvDontNeedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubToProtocol, Name: "recv_dont_need_at", Help: "recvDontNeedAt"}, labels),

		// fromProtocol
		fromProtocolRecvEvents: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_events", Help: "recvEvents"}, labels),
		fromProtocolRecvHashes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_hashes", Help: "recvHashes"}, labels),
		fromProtocolRecvDevInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_dev_info", Help: "recvDevInfo"}, labels),
		fromProtocolRecvAltSources: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_alt_sources", Help: "recvAltSources"}, labels),
		fromProtocolRecvReadAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_read_at", Help: "recvReadAt"}, labels),
		fromProtocolRecvWriteAtHash: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_write_at_hash", Help: "recvWriteAtHash"}, labels),
		fromProtocolRecvWriteAtComp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_write_at_comp", Help: "recvWriteAtComp"}, labels),
		fromProtocolRecvWriteAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_write_at", Help: "recvWriteAt"}, labels),
		fromProtocolRecvWriteAtWithMap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_write_at_with_map", Help: "recvWriteAtWithMap"}, labels),
		fromProtocolRecvRemoveFromMap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_remove_from_map", Help: "recvRemoveFromMap"}, labels),
		fromProtocolRecvRemoveDev: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_remove_dev", Help: "recvRemoveDev"}, labels),
		fromProtocolRecvDirtyList: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "recv_dirty_list", Help: "recvDirtyList"}, labels),
		fromProtocolSentNeedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "sent_need_at", Help: "sentNeedAt"}, labels),
		fromProtocolSentDontNeedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "sent_dont_need_at", Help: "sentDontNeedAt"}, labels),
		fromProtocolWritesAllowedP2P: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "writes_allowed_p2p", Help: "writesAllowedP2P"}, labels),
		fromProtocolWritesBlockedP2P: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "writes_blocked_p2p", Help: "writesBlockedP2P"}, labels),
		fromProtocolWritesAllowedAltSources: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "writes_allowed_alt_sources", Help: "writesAllowedAltSources"}, labels),
		fromProtocolWritesBlockedAltSources: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "writes_blocked_alt_sources", Help: "writesBlockedAltSources"}, labels),
		fromProtocolHeatmap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubFromProtocol, Name: "heatmap", Help: "Heatmap"}, append(labels, "le")),

		// S3Storage
		s3BlocksW: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubS3, Name: "blocks_w", Help: "Blocks w"}, labels),
		s3BlocksWBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubS3, Name: "blocks_w_bytes", Help: "Blocks w bytes"}, labels),
		s3BlocksR: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubS3, Name: "blocks_r", Help: "Blocks r"}, labels),
		s3BlocksRBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubS3, Name: "blocks_r_bytes", Help: "Blocks r bytes"}, labels),
		s3ActiveReads: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubS3, Name: "active_reads", Help: "Active reads"}, labels),
		s3ActiveWrites: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubS3, Name: "active_writes", Help: "Active writes"}, labels),

		// DirtyTracker
		dirtyTrackerBlockSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubDirtyTracker, Name: "block_size", Help: "Block size"}, labels),
		dirtyTrackerTrackingBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubDirtyTracker, Name: "tracking_blocks", Help: "Blocks being tracked"}, labels),
		dirtyTrackerDirtyBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubDirtyTracker, Name: "dirty_blocks", Help: "Blocks dirty"}, labels),
		dirtyTrackerMaxAgeDirtyMS: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubDirtyTracker, Name: "block_max_age", Help: "Block dirty max age"}, labels),

		// VolatilityMonitor
		volatilityMonitorBlockSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubVolatilityMonitor, Name: "block_size", Help: "Block size"}, labels),
		volatilityMonitorAvailable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubVolatilityMonitor, Name: "available", Help: "Blocks available"}, labels),
		volatilityMonitorVolatility: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubVolatilityMonitor, Name: "volatility", Help: "Volatility"}, labels),
		volatilityMonitorHeatmap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubVolatilityMonitor, Name: "heatmap", Help: "Heatmap"}, append(labels, "le")),

		// Metrics
		metricsReadOps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "read_ops", Help: "ReadOps"}, labels),
		metricsReadBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "read_bytes", Help: "ReadBytes"}, labels),
		metricsReadErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "read_errors", Help: "ReadErrors"}, labels),
		metricsReadTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "read_time", Help: "ReadTime"}, labels),
		metricsWriteOps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "write_ops", Help: "WriteOps"}, labels),
		metricsWriteBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "write_bytes", Help: "WriteBytes"}, labels),
		metricsWriteErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "write_errors", Help: "WriteErrors"}, labels),
		metricsWriteTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "write_time", Help: "WriteTime"}, labels),
		metricsFlushOps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "flush_ops", Help: "FlushOps"}, labels),
		metricsFlushErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "flush_errors", Help: "FlushErrors"}, labels),
		metricsFlushTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubMetrics, Name: "flush_time", Help: "FlushTime"}, labels),

		// nbd
		nbdPacketsIn: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubNBD, Name: "packets_in", Help: "PacketsIn"}, labels),
		nbdPacketsOut: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubNBD, Name: "packets_out", Help: "PacketsOut"}, labels),
		nbdReadAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubNBD, Name: "read_at", Help: "ReadAt"}, labels),
		nbdReadAtBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubNBD, Name: "read_at_bytes", Help: "ReadAtBytes"}, labels),
		nbdWriteAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubNBD, Name: "write_at", Help: "WriteAt"}, labels),
		nbdWriteAtBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubNBD, Name: "write_at_bytes", Help: "WriteAtBytes"}, labels),

		// waitingCache
		waitingCacheWaitForBlock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "waiting_for_block", Help: "WaitingForBlock"}, labels),
		waitingCacheWaitForBlockHadRemote: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "waiting_for_block_had_remote", Help: "WaitingForBlockHadRemote"}, labels),
		waitingCacheWaitForBlockHadLocal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "waiting_for_block_had_local", Help: "WaitingForBlockHadLocal"}, labels),
		waitingCacheWaitForBlockLock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "waiting_for_block_lock", Help: "WaitingForBlockLock"}, labels),
		waitingCacheWaitForBlockLockDone: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "waiting_for_block_lock_done", Help: "WaitingForBlockLockDone"}, labels),
		waitingCacheMarkAvailableLocalBlock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "mark_available_local_block", Help: "MarkAvailableLocalBlock"}, labels),
		waitingCacheMarkAvailableRemoteBlock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "mark_available_remote_block", Help: "MarkAvailableRemoteBlock"}, labels),
		waitingCacheAvailableLocal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "available_local", Help: "AvailableLocal"}, labels),
		waitingCacheAvailableRemote: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.Namespace, Subsystem: config.SubWaitingCache, Name: "available_remote", Help: "AvailableRemote"}, labels),

		cancelfns: make(map[string]context.CancelFunc),
	}

	// Register all the metrics
	reg.MustRegister(met.syncerBlockSize, met.syncerActiveBlocks, met.syncerTotalBlocks, met.syncerMigratedBlocks, met.syncerTotalMigratedBlocks, met.syncerReadyBlocks)

	reg.MustRegister(met.migratorBlockSize, met.migratorActiveBlocks, met.migratorTotalBlocks, met.migratorMigratedBlocks, met.migratorTotalMigratedBlocks, met.migratorReadyBlocks)

	reg.MustRegister(met.protocolActivePacketsSending, met.protocolPacketsSent, met.protocolDataSent, met.protocolPacketsRecv, met.protocolDataRecv, met.protocolWrites, met.protocolWriteErrors, met.protocolWaitingForID)

	reg.MustRegister(met.s3BlocksR, met.s3BlocksRBytes, met.s3BlocksW, met.s3BlocksWBytes, met.s3ActiveReads, met.s3ActiveWrites)

	reg.MustRegister(met.toProtocolSentEvents, met.toProtocolSentAltSources, met.toProtocolSentHashes, met.toProtocolSentDevInfo,
		met.toProtocolSentDirtyList, met.toProtocolSentReadAt, met.toProtocolSentWriteAtHash, met.toProtocolSentWriteAtHashBytes,
		met.toProtocolSentWriteAtComp, met.toProtocolSentWriteAtCompBytes, met.toProtocolSentWriteAtCompDataBytes,
		met.toProtocolSentWriteAt, met.toProtocolSentWriteAtBytes, met.toProtocolSentWriteAtWithMap,
		met.toProtocolSentRemoveFromMap, met.toProtocolSentYouAlreadyHave, met.toProtocolSentYouAlreadyHaveBytes,
		met.toProtocolRecvNeedAt, met.toProtocolRecvDontNeedAt,
	)

	reg.MustRegister(met.fromProtocolRecvEvents, met.fromProtocolRecvHashes, met.fromProtocolRecvDevInfo,
		met.fromProtocolRecvAltSources, met.fromProtocolRecvReadAt, met.fromProtocolRecvWriteAtHash,
		met.fromProtocolRecvWriteAtComp, met.fromProtocolRecvWriteAt, met.fromProtocolRecvWriteAtWithMap,
		met.fromProtocolRecvRemoveFromMap, met.fromProtocolRecvRemoveDev, met.fromProtocolRecvDirtyList,
		met.fromProtocolSentNeedAt, met.fromProtocolSentDontNeedAt,
		met.fromProtocolWritesAllowedP2P, met.fromProtocolWritesBlockedP2P,
		met.fromProtocolWritesAllowedAltSources, met.fromProtocolWritesBlockedAltSources,
		met.fromProtocolHeatmap)

	reg.MustRegister(met.dirtyTrackerBlockSize, met.dirtyTrackerDirtyBlocks, met.dirtyTrackerTrackingBlocks, met.dirtyTrackerMaxAgeDirtyMS)

	reg.MustRegister(met.volatilityMonitorBlockSize, met.volatilityMonitorAvailable, met.volatilityMonitorVolatility, met.volatilityMonitorHeatmap)

	reg.MustRegister(
		met.metricsReadOps,
		met.metricsReadBytes,
		met.metricsReadTime,
		met.metricsReadErrors,
		met.metricsWriteOps,
		met.metricsWriteBytes,
		met.metricsWriteTime,
		met.metricsWriteErrors,
		met.metricsFlushOps,
		met.metricsFlushTime,
		met.metricsFlushErrors)

	reg.MustRegister(
		met.nbdPacketsIn, met.nbdPacketsOut, met.nbdReadAt, met.nbdReadAtBytes, met.nbdWriteAt, met.nbdWriteAtBytes)

	reg.MustRegister(
		met.waitingCacheWaitForBlock,
		met.waitingCacheWaitForBlockHadRemote,
		met.waitingCacheWaitForBlockHadLocal,
		met.waitingCacheWaitForBlockLock,
		met.waitingCacheWaitForBlockLockDone,
		met.waitingCacheMarkAvailableLocalBlock,
		met.waitingCacheMarkAvailableRemoteBlock,
		met.waitingCacheAvailableLocal,
		met.waitingCacheAvailableRemote,
	)

	return met
}

func (m *Metrics) remove(subsystem string, id string, name string) {
	m.lock.Lock()
	cancelfn, ok := m.cancelfns[fmt.Sprintf("%s_%s_%s", subsystem, id, name)]
	if ok {
		cancelfn()
		delete(m.cancelfns, fmt.Sprintf("%s_%s_%s", subsystem, id, name))
	}
	m.lock.Unlock()
}

func (m *Metrics) add(subsystem string, id string, name string, interval time.Duration, tickfn func()) {
	ctx, cancelfn := context.WithCancel(context.TODO())
	m.lock.Lock()
	m.cancelfns[fmt.Sprintf("%s_%s_%s", subsystem, id, name)] = cancelfn
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

// Shutdown everything
func (m *Metrics) Shutdown() {
	m.lock.Lock()
	for _, cancelfn := range m.cancelfns {
		cancelfn()
	}
	m.cancelfns = make(map[string]context.CancelFunc)
	m.lock.Unlock()
}

func (m *Metrics) AddSyncer(id string, name string, syncer *migrator.Syncer) {
	m.add(m.config.SubSyncer, id, name, m.config.TickSyncer, func() {
		met := syncer.GetMetrics()
		if met != nil {
			m.migratorBlockSize.WithLabelValues(id, name).Set(float64(met.BlockSize))
			m.migratorTotalBlocks.WithLabelValues(id, name).Set(float64(met.TotalBlocks))
			m.migratorMigratedBlocks.WithLabelValues(id, name).Set(float64(met.MigratedBlocks))
			m.migratorReadyBlocks.WithLabelValues(id, name).Set(float64(met.ReadyBlocks))
			m.migratorActiveBlocks.WithLabelValues(id, name).Set(float64(met.ActiveBlocks))
			m.migratorTotalMigratedBlocks.WithLabelValues(id, name).Set(float64(met.TotalMigratedBlocks))
		}
	})
}

func (m *Metrics) RemoveSyncer(id string, name string) {
	m.remove(m.config.SubSyncer, id, name)
}

func (m *Metrics) AddMigrator(id string, name string, mig *migrator.Migrator) {
	m.add(m.config.SubMigrator, id, name, m.config.TickMigrator, func() {
		met := mig.GetMetrics()
		m.migratorBlockSize.WithLabelValues(id, name).Set(float64(met.BlockSize))
		m.migratorTotalBlocks.WithLabelValues(id, name).Set(float64(met.TotalBlocks))
		m.migratorMigratedBlocks.WithLabelValues(id, name).Set(float64(met.MigratedBlocks))
		m.migratorReadyBlocks.WithLabelValues(id, name).Set(float64(met.ReadyBlocks))
		m.migratorActiveBlocks.WithLabelValues(id, name).Set(float64(met.ActiveBlocks))
		m.migratorTotalMigratedBlocks.WithLabelValues(id, name).Set(float64(met.TotalMigratedBlocks))
	})
}

func (m *Metrics) RemoveMigrator(id string, name string) {
	m.remove(m.config.SubMigrator, id, name)
}

func (m *Metrics) AddProtocol(id string, name string, proto *protocol.RW) {
	m.add(m.config.SubProtocol, id, name, m.config.TickProtocol, func() {
		met := proto.GetMetrics()
		m.protocolActivePacketsSending.WithLabelValues(id, name).Set(float64(met.ActivePacketsSending))
		m.protocolPacketsSent.WithLabelValues(id, name).Set(float64(met.PacketsSent))
		m.protocolDataSent.WithLabelValues(id, name).Set(float64(met.DataSent))
		m.protocolPacketsRecv.WithLabelValues(id, name).Set(float64(met.PacketsRecv))
		m.protocolDataRecv.WithLabelValues(id, name).Set(float64(met.DataRecv))
		m.protocolWrites.WithLabelValues(id, name).Set(float64(met.Writes))
		m.protocolWriteErrors.WithLabelValues(id, name).Set(float64(met.WriteErrors))
		m.protocolWaitingForID.WithLabelValues(id, name).Set(float64(met.WaitingForID))
	})
}

func (m *Metrics) RemoveProtocol(id string, name string) {
	m.remove(m.config.SubProtocol, id, name)
}

func (m *Metrics) AddToProtocol(id string, name string, proto *protocol.ToProtocol) {
	m.add(m.config.SubToProtocol, id, name, m.config.TickToProtocol, func() {
		met := proto.GetMetrics()

		m.toProtocolSentEvents.WithLabelValues(id, name).Set(float64(met.SentEvents))
		m.toProtocolSentAltSources.WithLabelValues(id, name).Set(float64(met.SentAltSources))
		m.toProtocolSentHashes.WithLabelValues(id, name).Set(float64(met.SentHashes))
		m.toProtocolSentDevInfo.WithLabelValues(id, name).Set(float64(met.SentDevInfo))
		m.toProtocolSentDirtyList.WithLabelValues(id, name).Set(float64(met.SentDirtyList))
		m.toProtocolSentReadAt.WithLabelValues(id, name).Set(float64(met.SentReadAt))
		m.toProtocolSentWriteAtHash.WithLabelValues(id, name).Set(float64(met.SentWriteAtHash))
		m.toProtocolSentWriteAtHashBytes.WithLabelValues(id, name).Set(float64(met.SentWriteAtHashBytes))
		m.toProtocolSentWriteAtComp.WithLabelValues(id, name).Set(float64(met.SentWriteAtComp))
		m.toProtocolSentWriteAtCompBytes.WithLabelValues(id, name).Set(float64(met.SentWriteAtCompBytes))
		m.toProtocolSentWriteAtCompDataBytes.WithLabelValues(id, name).Set(float64(met.SentWriteAtCompDataBytes))
		m.toProtocolSentWriteAt.WithLabelValues(id, name).Set(float64(met.SentWriteAt))
		m.toProtocolSentWriteAtBytes.WithLabelValues(id, name).Set(float64(met.SentWriteAtBytes))
		m.toProtocolSentWriteAtWithMap.WithLabelValues(id, name).Set(float64(met.SentWriteAtWithMap))
		m.toProtocolSentRemoveFromMap.WithLabelValues(id, name).Set(float64(met.SentRemoveFromMap))
		m.toProtocolSentYouAlreadyHave.WithLabelValues(id, name).Set(float64(met.SentYouAlreadyHave))
		m.toProtocolSentYouAlreadyHaveBytes.WithLabelValues(id, name).Set(float64(met.SentYouAlreadyHaveBytes))
		m.toProtocolRecvNeedAt.WithLabelValues(id, name).Set(float64(met.RecvNeedAt))
		m.toProtocolRecvDontNeedAt.WithLabelValues(id, name).Set(float64(met.RecvDontNeedAt))
	})
}

func (m *Metrics) RemoveToProtocol(id string, name string) {
	m.remove(m.config.SubToProtocol, id, name)
}

func (m *Metrics) AddFromProtocol(id string, name string, proto *protocol.FromProtocol) {
	m.add(m.config.SubFromProtocol, id, name, m.config.TickFromProtocol, func() {
		met := proto.GetMetrics()

		if met.DeviceName != "" {
			name = met.DeviceName
		}

		m.fromProtocolRecvEvents.WithLabelValues(id, name).Set(float64(met.RecvEvents))
		m.fromProtocolRecvHashes.WithLabelValues(id, name).Set(float64(met.RecvHashes))
		m.fromProtocolRecvDevInfo.WithLabelValues(id, name).Set(float64(met.RecvDevInfo))
		m.fromProtocolRecvAltSources.WithLabelValues(id, name).Set(float64(met.RecvAltSources))
		m.fromProtocolRecvReadAt.WithLabelValues(id, name).Set(float64(met.RecvReadAt))
		m.fromProtocolRecvWriteAtHash.WithLabelValues(id, name).Set(float64(met.RecvWriteAtHash))
		m.fromProtocolRecvWriteAtComp.WithLabelValues(id, name).Set(float64(met.RecvWriteAtComp))
		m.fromProtocolRecvWriteAt.WithLabelValues(id, name).Set(float64(met.RecvWriteAt))
		m.fromProtocolRecvWriteAtWithMap.WithLabelValues(id, name).Set(float64(met.RecvWriteAtWithMap))
		m.fromProtocolRecvRemoveFromMap.WithLabelValues(id, name).Set(float64(met.RecvRemoveFromMap))
		m.fromProtocolRecvRemoveDev.WithLabelValues(id, name).Set(float64(met.RecvRemoveDev))
		m.fromProtocolRecvDirtyList.WithLabelValues(id, name).Set(float64(met.RecvDirtyList))
		m.fromProtocolSentNeedAt.WithLabelValues(id, name).Set(float64(met.SentNeedAt))
		m.fromProtocolSentDontNeedAt.WithLabelValues(id, name).Set(float64(met.SentDontNeedAt))

		m.fromProtocolWritesAllowedP2P.WithLabelValues(id, name).Set(float64(met.WritesAllowedP2P))
		m.fromProtocolWritesBlockedP2P.WithLabelValues(id, name).Set(float64(met.WritesBlockedP2P))
		m.fromProtocolWritesAllowedAltSources.WithLabelValues(id, name).Set(float64(met.WritesAllowedAltSources))
		m.fromProtocolWritesBlockedAltSources.WithLabelValues(id, name).Set(float64(met.WritesBlockedAltSources))

		totalHeatmapP2P := make([]uint64, m.config.HeatmapResolution)
		for _, block := range met.AvailableP2P {
			part := uint64(block) * m.config.HeatmapResolution / met.NumBlocks
			totalHeatmapP2P[part]++
		}

		totalHeatmapAltSources := make([]uint64, m.config.HeatmapResolution)
		for _, block := range met.AvailableAltSources {
			part := uint64(block) * m.config.HeatmapResolution / met.NumBlocks
			totalHeatmapAltSources[part]++
		}

		totalHeatmapP2PDupe := make([]uint64, m.config.HeatmapResolution)
		for _, block := range met.DuplicateP2P {
			part := uint64(block) * m.config.HeatmapResolution / met.NumBlocks
			totalHeatmapP2PDupe[part]++
		}

		blocksPerPart := 2 * (met.NumBlocks / m.config.HeatmapResolution)

		//
		for part, blocks := range totalHeatmapP2P {
			m.fromProtocolHeatmap.WithLabelValues(id, name, fmt.Sprintf("%d", part)).Set(float64(blocks))
		}

		for part, blocks := range totalHeatmapAltSources {
			if blocks > 0 {
				m.fromProtocolHeatmap.WithLabelValues(id, name, fmt.Sprintf("%d", part)).Set(float64(blocksPerPart*2 + blocks))
			}
		}

		for part, blocks := range totalHeatmapP2PDupe {
			if blocks > 0 {
				m.fromProtocolHeatmap.WithLabelValues(id, name, fmt.Sprintf("%d", part)).Set(float64(blocksPerPart + blocks))
			}
		}

	})
}

func (m *Metrics) RemoveFromProtocol(id string, name string) {
	m.remove(m.config.SubFromProtocol, id, name)
}

func (m *Metrics) AddS3Storage(id string, name string, s3 *sources.S3Storage) {
	m.add(m.config.SubS3, id, name, m.config.TickS3, func() {
		met := s3.Metrics()
		m.s3BlocksW.WithLabelValues(id, name).Set(float64(met.BlocksWCount))
		m.s3BlocksWBytes.WithLabelValues(id, name).Set(float64(met.BlocksWBytes))
		m.s3BlocksR.WithLabelValues(id, name).Set(float64(met.BlocksRCount))
		m.s3BlocksRBytes.WithLabelValues(id, name).Set(float64(met.BlocksRBytes))
		m.s3ActiveReads.WithLabelValues(id, name).Set(float64(met.ActiveReads))
		m.s3ActiveWrites.WithLabelValues(id, name).Set(float64(met.ActiveWrites))
	})

}

func (m *Metrics) RemoveS3Storage(id string, name string) {
	m.remove(m.config.SubS3, id, name)
}

func (m *Metrics) AddDirtyTracker(id string, name string, dt *dirtytracker.Remote) {
	m.add(m.config.SubDirtyTracker, id, name, m.config.TickDirtyTracker, func() {
		met := dt.GetMetrics()
		m.dirtyTrackerBlockSize.WithLabelValues(id, name).Set(float64(met.BlockSize))
		m.dirtyTrackerTrackingBlocks.WithLabelValues(id, name).Set(float64(met.TrackingBlocks))
		m.dirtyTrackerDirtyBlocks.WithLabelValues(id, name).Set(float64(met.DirtyBlocks))
		m.dirtyTrackerMaxAgeDirtyMS.WithLabelValues(id, name).Set(float64(met.MaxAgeDirty))
	})
}

func (m *Metrics) RemoveDirtyTracker(id string, name string) {
	m.remove(m.config.SubDirtyTracker, id, name)
}

func (m *Metrics) AddVolatilityMonitor(id string, name string, vm *volatilitymonitor.VolatilityMonitor) {
	m.add(m.config.SubVolatilityMonitor, id, name, m.config.TickVolatilityMonitor, func() {
		met := vm.GetMetrics()
		m.volatilityMonitorBlockSize.WithLabelValues(id, name).Set(float64(met.BlockSize))
		m.volatilityMonitorAvailable.WithLabelValues(id, name).Set(float64(met.Available))
		m.volatilityMonitorVolatility.WithLabelValues(id, name).Set(float64(met.Volatility))

		totalVolatility := make([]uint64, m.config.HeatmapResolution)
		for block, volatility := range met.VolatilityMap {
			part := uint64(block) * m.config.HeatmapResolution / met.NumBlocks
			totalVolatility[part] += volatility
		}

		for part, volatility := range totalVolatility {
			m.volatilityMonitorHeatmap.WithLabelValues(id, name, fmt.Sprintf("%d", part)).Set(float64(volatility))
		}
	})
}

func (m *Metrics) RemoveVolatilityMonitor(id string, name string) {
	m.remove(m.config.SubVolatilityMonitor, id, name)
}

func (m *Metrics) AddMetrics(id string, name string, mm *modules.Metrics) {
	m.add(m.config.SubMetrics, id, name, m.config.TickMetrics, func() {
		met := mm.GetMetrics()
		m.metricsReadOps.WithLabelValues(id, name).Set(float64(met.ReadOps))
		m.metricsReadBytes.WithLabelValues(id, name).Set(float64(met.ReadBytes))
		m.metricsReadErrors.WithLabelValues(id, name).Set(float64(met.ReadErrors))
		m.metricsReadTime.WithLabelValues(id, name).Set(float64(met.ReadTime))
		m.metricsWriteOps.WithLabelValues(id, name).Set(float64(met.WriteOps))
		m.metricsWriteBytes.WithLabelValues(id, name).Set(float64(met.WriteBytes))
		m.metricsWriteErrors.WithLabelValues(id, name).Set(float64(met.WriteErrors))
		m.metricsWriteTime.WithLabelValues(id, name).Set(float64(met.WriteTime))
		m.metricsFlushOps.WithLabelValues(id, name).Set(float64(met.FlushOps))
		m.metricsFlushErrors.WithLabelValues(id, name).Set(float64(met.FlushErrors))
		m.metricsFlushTime.WithLabelValues(id, name).Set(float64(met.FlushTime))
	})
}

func (m *Metrics) RemoveMetrics(id string, name string) {
	m.remove(m.config.SubMetrics, id, name)
}

func (m *Metrics) AddNBD(id string, name string, mm *expose.ExposedStorageNBDNL) {
	m.add(m.config.SubNBD, id, name, m.config.TickNBD, func() {
		met := mm.GetMetrics()
		m.nbdPacketsIn.WithLabelValues(id, name).Set(float64(met.PacketsIn))
		m.nbdPacketsOut.WithLabelValues(id, name).Set(float64(met.PacketsOut))
		m.nbdReadAt.WithLabelValues(id, name).Set(float64(met.ReadAt))
		m.nbdReadAtBytes.WithLabelValues(id, name).Set(float64(met.ReadAtBytes))
		m.nbdWriteAt.WithLabelValues(id, name).Set(float64(met.WriteAt))
		m.nbdWriteAtBytes.WithLabelValues(id, name).Set(float64(met.WriteAtBytes))
	})
}

func (m *Metrics) RemoveNBD(id string, name string) {
	m.remove(m.config.SubNBD, id, name)
}

func (m *Metrics) AddWaitingCache(id string, name string, wc *waitingcache.Remote) {
	m.add(m.config.SubWaitingCache, id, name, m.config.TickWaitingCache, func() {
		met := wc.GetMetrics()
		m.waitingCacheWaitForBlock.WithLabelValues(id, name).Set(float64(met.WaitForBlock))
		m.waitingCacheWaitForBlockHadRemote.WithLabelValues(id, name).Set(float64(met.WaitForBlockHadRemote))
		m.waitingCacheWaitForBlockHadLocal.WithLabelValues(id, name).Set(float64(met.WaitForBlockHadLocal))
		m.waitingCacheWaitForBlockLock.WithLabelValues(id, name).Set(float64(met.WaitForBlockLock))
		m.waitingCacheWaitForBlockLockDone.WithLabelValues(id, name).Set(float64(met.WaitForBlockLockDone))
		m.waitingCacheMarkAvailableLocalBlock.WithLabelValues(id, name).Set(float64(met.MarkAvailableLocalBlock))
		m.waitingCacheMarkAvailableRemoteBlock.WithLabelValues(id, name).Set(float64(met.MarkAvailableRemoteBlock))
		m.waitingCacheAvailableLocal.WithLabelValues(id, name).Set(float64(met.AvailableLocal))
		m.waitingCacheAvailableRemote.WithLabelValues(id, name).Set(float64(met.AvailableRemote))
	})
}

func (m *Metrics) RemoveWaitingCache(id string, name string) {
	m.remove(m.config.SubWaitingCache, id, name)
}
