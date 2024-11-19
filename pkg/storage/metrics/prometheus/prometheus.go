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

// How often to poll metrics. We may want to do some less often etc
const migratorTick = 100 * time.Millisecond
const syncerTick = 100 * time.Millisecond
const protocolTick = 100 * time.Millisecond
const toProtocolTick = 100 * time.Millisecond
const fromProtocolTick = 100 * time.Millisecond
const s3Tick = 100 * time.Millisecond
const dirtyTrackerTick = 100 * time.Millisecond
const volatilityMonitorTick = 100 * time.Millisecond
const metricsTick = 100 * time.Millisecond
const nbdTick = 100 * time.Millisecond
const waitingCacheTick = 100 * time.Millisecond

const promNamespace = "silo"

const promSubSyncer = "syncer"
const promSubMigrator = "migrator"
const promSubProtocol = "protocol"
const promSubToProtocol = "toProtocol"
const promSubFromProtocol = "fromProtocol"
const promSubS3 = "s3"
const promSubDirtyTracker = "dirtyTracker"
const promSubVolatilityMonitor = "volatilityMonitor"
const promSubMetrics = "metrics"
const promSubNbd = "nbd"
const promSubWaitingCache = "waitingCache"

type Metrics struct {
	reg  prometheus.Registerer
	lock sync.Mutex

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
	protocolPacketsSent  *prometheus.GaugeVec
	protocolDataSent     *prometheus.GaugeVec
	protocolPacketsRecv  *prometheus.GaugeVec
	protocolDataRecv     *prometheus.GaugeVec
	protocolWrites       *prometheus.GaugeVec
	protocolWriteErrors  *prometheus.GaugeVec
	protocolWaitingForId *prometheus.GaugeVec

	// s3
	s3BlocksR      *prometheus.GaugeVec
	s3BlocksRBytes *prometheus.GaugeVec
	s3BlocksW      *prometheus.GaugeVec
	s3BlocksWBytes *prometheus.GaugeVec

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
	toProtocolRecvNeedAt               *prometheus.GaugeVec
	toProtocolRecvDontNeedAt           *prometheus.GaugeVec

	// fromProtocol
	fromProtocolRecvEvents         *prometheus.GaugeVec
	fromProtocolRecvHashes         *prometheus.GaugeVec
	fromProtocolRecvDevInfo        *prometheus.GaugeVec
	fromProtocolRecvAltSources     *prometheus.GaugeVec
	fromProtocolRecvReadAt         *prometheus.GaugeVec
	fromProtocolRecvWriteAtHash    *prometheus.GaugeVec
	fromProtocolRecvWriteAtComp    *prometheus.GaugeVec
	fromProtocolRecvWriteAt        *prometheus.GaugeVec
	fromProtocolRecvWriteAtWithMap *prometheus.GaugeVec
	fromProtocolRecvRemoveFromMap  *prometheus.GaugeVec
	fromProtocolRecvRemoveDev      *prometheus.GaugeVec
	fromProtocolRecvDirtyList      *prometheus.GaugeVec
	fromProtocolSentNeedAt         *prometheus.GaugeVec
	fromProtocolSentDontNeedAt     *prometheus.GaugeVec

	// dirtyTracker
	dirtyTrackerBlockSize      *prometheus.GaugeVec
	dirtyTrackerTrackingBlocks *prometheus.GaugeVec
	dirtyTrackerDirtyBlocks    *prometheus.GaugeVec
	dirtyTrackerMaxAgeDirtyMS  *prometheus.GaugeVec

	// volatilityMonitor
	volatilityMonitorBlockSize  *prometheus.GaugeVec
	volatilityMonitorAvailable  *prometheus.GaugeVec
	volatilityMonitorVolatility *prometheus.GaugeVec

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

	// vm
	vmRunning prometheus.Gauge

	cancelfns map[string]context.CancelFunc
}

func New(reg prometheus.Registerer) *Metrics {
	met := &Metrics{
		reg: reg,
		// Syncer
		syncerBlockSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubSyncer, Name: "block_size", Help: "Block size"}, []string{"device"}),
		syncerTotalBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubSyncer, Name: "total_blocks", Help: "Total blocks"}, []string{"device"}),
		syncerActiveBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubSyncer, Name: "active_blocks", Help: "Active blocks"}, []string{"device"}),
		syncerMigratedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubSyncer, Name: "migrated_blocks", Help: "Migrated blocks"}, []string{"device"}),
		syncerReadyBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubSyncer, Name: "ready_blocks", Help: "Ready blocks"}, []string{"device"}),
		syncerTotalMigratedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubSyncer, Name: "total_migrated_blocks", Help: "Total migrated blocks"}, []string{"device"}),

		// Migrator
		migratorBlockSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMigrator, Name: "block_size", Help: "Block size"}, []string{"device"}),
		migratorTotalBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMigrator, Name: "total_blocks", Help: "Total blocks"}, []string{"device"}),
		migratorActiveBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMigrator, Name: "active_blocks", Help: "Active blocks"}, []string{"device"}),
		migratorMigratedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMigrator, Name: "migrated_blocks", Help: "Migrated blocks"}, []string{"device"}),
		migratorReadyBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMigrator, Name: "ready_blocks", Help: "Ready blocks"}, []string{"device"}),
		migratorTotalMigratedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMigrator, Name: "total_migrated_blocks", Help: "Total migrated blocks"}, []string{"device"}),

		// Protocol
		protocolPacketsSent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubProtocol, Name: "packets_sent", Help: "Packets sent"}, []string{"device"}),
		protocolDataSent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubProtocol, Name: "data_sent", Help: "Data sent"}, []string{"device"}),
		protocolPacketsRecv: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubProtocol, Name: "packets_recv", Help: "Packets recv"}, []string{"device"}),
		protocolDataRecv: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubProtocol, Name: "data_recv", Help: "Data recv"}, []string{"device"}),
		protocolWrites: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubProtocol, Name: "writes", Help: "Writes"}, []string{"device"}),
		protocolWriteErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubProtocol, Name: "write_errors", Help: "Write errors"}, []string{"device"}),
		protocolWaitingForId: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubProtocol, Name: "waiting_for_id", Help: "Waiting for ID"}, []string{"device"}),

		// ToProtocol
		toProtocolSentEvents: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "sent_events", Help: "sentEvents"}, []string{"device"}),
		toProtocolSentAltSources: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "sent_alt_sources", Help: "sentAltSources"}, []string{"device"}),
		toProtocolSentHashes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "sent_hashes", Help: "sentHashes"}, []string{"device"}),
		toProtocolSentDevInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "sent_dev_info", Help: "sentDevInfo"}, []string{"device"}),
		toProtocolSentDirtyList: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "sent_dirty_list", Help: "sentDirtyList"}, []string{"device"}),
		toProtocolSentReadAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "sent_read_at", Help: "sentReadAt"}, []string{"device"}),
		toProtocolSentWriteAtHash: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "sent_write_at_hash", Help: "sentWriteAtHash"}, []string{"device"}),
		toProtocolSentWriteAtHashBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "write_at_hash_bytes", Help: "sentWriteAtHashBytes"}, []string{"device"}),
		toProtocolSentWriteAtComp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "write_at_comp", Help: "sentWriteAtComp"}, []string{"device"}),
		toProtocolSentWriteAtCompBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "write_at_comp_bytes", Help: "sentWriteAtCompBytes"}, []string{"device"}),
		toProtocolSentWriteAtCompDataBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "write_at_comp_data_bytes", Help: "sentWriteAtCompDataBytes"}, []string{"device"}),
		toProtocolSentWriteAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "write_at", Help: "sentWriteAt"}, []string{"device"}),
		toProtocolSentWriteAtBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "write_at_bytes", Help: "sentWriteAtBytes"}, []string{"device"}),
		toProtocolSentWriteAtWithMap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "write_at_with_map", Help: "sentWriteAtWithMap"}, []string{"device"}),
		toProtocolSentRemoveFromMap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "sent_remove_from_map", Help: "sentRemoveFromMap"}, []string{"device"}),
		toProtocolRecvNeedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "recv_need_at", Help: "recvNeedAt"}, []string{"device"}),
		toProtocolRecvDontNeedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubToProtocol, Name: "recv_dont_need_at", Help: "recvDontNeedAt"}, []string{"device"}),

		// fromProtocol
		fromProtocolRecvEvents: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_events", Help: "recvEvents"}, []string{"device"}),
		fromProtocolRecvHashes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_hashes", Help: "recvHashes"}, []string{"device"}),
		fromProtocolRecvDevInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_dev_info", Help: "recvDevInfo"}, []string{"device"}),
		fromProtocolRecvAltSources: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_alt_sources", Help: "recvAltSources"}, []string{"device"}),
		fromProtocolRecvReadAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_read_at", Help: "recvReadAt"}, []string{"device"}),
		fromProtocolRecvWriteAtHash: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_write_at_hash", Help: "recvWriteAtHash"}, []string{"device"}),
		fromProtocolRecvWriteAtComp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_write_at_comp", Help: "recvWriteAtComp"}, []string{"device"}),
		fromProtocolRecvWriteAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_write_at", Help: "recvWriteAt"}, []string{"device"}),
		fromProtocolRecvWriteAtWithMap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_write_at_with_map", Help: "recvWriteAtWithMap"}, []string{"device"}),
		fromProtocolRecvRemoveFromMap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_remove_from_map", Help: "recvRemoveFromMap"}, []string{"device"}),
		fromProtocolRecvRemoveDev: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_remove_dev", Help: "recvRemoveDev"}, []string{"device"}),
		fromProtocolRecvDirtyList: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "recv_dirty_list", Help: "recvDirtyList"}, []string{"device"}),
		fromProtocolSentNeedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "sent_need_at", Help: "sentNeedAt"}, []string{"device"}),
		fromProtocolSentDontNeedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubFromProtocol, Name: "sent_dont_need_at", Help: "sentDontNeedAt"}, []string{"device"}),

		// S3Storage
		s3BlocksW: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubS3, Name: "blocks_w", Help: "Blocks w"}, []string{"device"}),
		s3BlocksWBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubS3, Name: "blocks_w_bytes", Help: "Blocks w bytes"}, []string{"device"}),
		s3BlocksR: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubS3, Name: "blocks_r", Help: "Blocks r"}, []string{"device"}),
		s3BlocksRBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubS3, Name: "blocks_r_bytes", Help: "Blocks r bytes"}, []string{"device"}),

		// DirtyTracker
		dirtyTrackerBlockSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubDirtyTracker, Name: "block_size", Help: "Block size"}, []string{"device"}),
		dirtyTrackerTrackingBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubDirtyTracker, Name: "tracking_blocks", Help: "Blocks being tracked"}, []string{"device"}),
		dirtyTrackerDirtyBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubDirtyTracker, Name: "dirty_blocks", Help: "Blocks dirty"}, []string{"device"}),
		dirtyTrackerMaxAgeDirtyMS: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubDirtyTracker, Name: "block_max_age", Help: "Block dirty max age"}, []string{"device"}),

		// VolatilityMonitor
		volatilityMonitorBlockSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubVolatilityMonitor, Name: "block_size", Help: "Block size"}, []string{"device"}),
		volatilityMonitorAvailable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubVolatilityMonitor, Name: "available", Help: "Blocks available"}, []string{"device"}),
		volatilityMonitorVolatility: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubVolatilityMonitor, Name: "volatility", Help: "Volatility"}, []string{"device"}),

		// Metrics
		metricsReadOps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "read_ops", Help: "ReadOps"}, []string{"device"}),
		metricsReadBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "read_bytes", Help: "ReadBytes"}, []string{"device"}),
		metricsReadErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "read_errors", Help: "ReadErrors"}, []string{"device"}),
		metricsReadTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "read_time", Help: "ReadTime"}, []string{"device"}),
		metricsWriteOps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "write_ops", Help: "WriteOps"}, []string{"device"}),
		metricsWriteBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "write_bytes", Help: "WriteBytes"}, []string{"device"}),
		metricsWriteErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "write_errors", Help: "WriteErrors"}, []string{"device"}),
		metricsWriteTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "write_time", Help: "WriteTime"}, []string{"device"}),
		metricsFlushOps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "flush_ops", Help: "FlushOps"}, []string{"device"}),
		metricsFlushErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "flush_errors", Help: "FlushErrors"}, []string{"device"}),
		metricsFlushTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubMetrics, Name: "flush_time", Help: "FlushTime"}, []string{"device"}),

		// nbd
		nbdPacketsIn: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubNbd, Name: "packets_in", Help: "PacketsIn"}, []string{"device"}),
		nbdPacketsOut: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubNbd, Name: "packets_out", Help: "PacketsOut"}, []string{"device"}),
		nbdReadAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubNbd, Name: "read_at", Help: "ReadAt"}, []string{"device"}),
		nbdReadAtBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubNbd, Name: "read_at_bytes", Help: "ReadAtBytes"}, []string{"device"}),
		nbdWriteAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubNbd, Name: "write_at", Help: "WriteAt"}, []string{"device"}),
		nbdWriteAtBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubNbd, Name: "write_at_bytes", Help: "WriteAtBytes"}, []string{"device"}),

		// waitingCache
		waitingCacheWaitForBlock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "waiting_for_block", Help: "WaitingForBlock"}, []string{"device"}),
		waitingCacheWaitForBlockHadRemote: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "waiting_for_block_had_remote", Help: "WaitingForBlockHadRemote"}, []string{"device"}),
		waitingCacheWaitForBlockHadLocal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "waiting_for_block_had_local", Help: "WaitingForBlockHadLocal"}, []string{"device"}),
		waitingCacheWaitForBlockLock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "waiting_for_block_lock", Help: "WaitingForBlockLock"}, []string{"device"}),
		waitingCacheWaitForBlockLockDone: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "waiting_for_block_lock_done", Help: "WaitingForBlockLockDone"}, []string{"device"}),
		waitingCacheMarkAvailableLocalBlock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "mark_available_local_block", Help: "MarkAvailableLocalBlock"}, []string{"device"}),
		waitingCacheMarkAvailableRemoteBlock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "mark_available_remote_block", Help: "MarkAvailableRemoteBlock"}, []string{"device"}),
		waitingCacheAvailableLocal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "available_local", Help: "AvailableLocal"}, []string{"device"}),
		waitingCacheAvailableRemote: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace, Subsystem: promSubWaitingCache, Name: "available_remote", Help: "AvailableRemote"}, []string{"device"}),

		// vm
		vmRunning: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: promNamespace, Subsystem: "vm", Name: "running", Help: "running"}),

		cancelfns: make(map[string]context.CancelFunc),
	}

	// Register all the metrics
	reg.MustRegister(met.syncerBlockSize, met.syncerActiveBlocks, met.syncerTotalBlocks, met.syncerMigratedBlocks, met.syncerTotalMigratedBlocks, met.syncerReadyBlocks)

	reg.MustRegister(met.migratorBlockSize, met.migratorActiveBlocks, met.migratorTotalBlocks, met.migratorMigratedBlocks, met.migratorTotalMigratedBlocks, met.migratorReadyBlocks)

	reg.MustRegister(met.protocolPacketsSent, met.protocolDataSent, met.protocolPacketsRecv, met.protocolDataRecv, met.protocolWrites, met.protocolWriteErrors, met.protocolWaitingForId)

	reg.MustRegister(met.s3BlocksR, met.s3BlocksRBytes, met.s3BlocksW, met.s3BlocksWBytes)

	reg.MustRegister(met.toProtocolSentEvents, met.toProtocolSentAltSources, met.toProtocolSentHashes, met.toProtocolSentDevInfo,
		met.toProtocolSentDirtyList, met.toProtocolSentReadAt, met.toProtocolSentWriteAtHash, met.toProtocolSentWriteAtHashBytes,
		met.toProtocolSentWriteAtComp, met.toProtocolSentWriteAtCompBytes, met.toProtocolSentWriteAtCompDataBytes,
		met.toProtocolSentWriteAt, met.toProtocolSentWriteAtBytes, met.toProtocolSentWriteAtWithMap,
		met.toProtocolSentRemoveFromMap, met.toProtocolRecvNeedAt, met.toProtocolRecvDontNeedAt,
	)

	reg.MustRegister(met.fromProtocolRecvEvents, met.fromProtocolRecvHashes, met.fromProtocolRecvDevInfo,
		met.fromProtocolRecvAltSources, met.fromProtocolRecvReadAt, met.fromProtocolRecvWriteAtHash,
		met.fromProtocolRecvWriteAtComp, met.fromProtocolRecvWriteAt, met.fromProtocolRecvWriteAtWithMap,
		met.fromProtocolRecvRemoveFromMap, met.fromProtocolRecvRemoveDev, met.fromProtocolRecvDirtyList,
		met.fromProtocolSentNeedAt, met.fromProtocolSentDontNeedAt)

	reg.MustRegister(met.dirtyTrackerBlockSize, met.dirtyTrackerDirtyBlocks, met.dirtyTrackerTrackingBlocks, met.dirtyTrackerMaxAgeDirtyMS)

	reg.MustRegister(met.volatilityMonitorBlockSize, met.volatilityMonitorAvailable, met.volatilityMonitorVolatility)

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

	reg.MustRegister(met.vmRunning)
	return met
}

func (m *Metrics) remove(subsystem string, name string) {
	m.lock.Lock()
	cancelfn, ok := m.cancelfns[fmt.Sprintf("%s_%s", subsystem, name)]
	if ok {
		cancelfn()
		delete(m.cancelfns, fmt.Sprintf("%s_%s", subsystem, name))
	}
	m.lock.Unlock()
}

func (m *Metrics) add(subsystem string, name string, interval time.Duration, tickfn func()) {
	ctx, cancelfn := context.WithCancel(context.TODO())
	m.lock.Lock()
	m.cancelfns[fmt.Sprintf("%s_%s", subsystem, name)] = cancelfn
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

func (m *Metrics) AddSyncer(name string, syncer *migrator.Syncer) {
	m.add(promSubSyncer, name, syncerTick, func() {
		met := syncer.GetMetrics()
		m.migratorBlockSize.WithLabelValues(name).Set(float64(met.BlockSize))
		m.migratorTotalBlocks.WithLabelValues(name).Set(float64(met.TotalBlocks))
		m.migratorMigratedBlocks.WithLabelValues(name).Set(float64(met.MigratedBlocks))
		m.migratorReadyBlocks.WithLabelValues(name).Set(float64(met.ReadyBlocks))
		m.migratorActiveBlocks.WithLabelValues(name).Set(float64(met.ActiveBlocks))
		m.migratorTotalMigratedBlocks.WithLabelValues(name).Set(float64(met.TotalMigratedBlocks))
	})
}

func (m *Metrics) RemoveSyncer(name string) {
	m.remove(promSubSyncer, name)
}

func (m *Metrics) AddMigrator(name string, mig *migrator.Migrator) {
	m.add(promSubMigrator, name, migratorTick, func() {
		met := mig.GetMetrics()
		m.migratorBlockSize.WithLabelValues(name).Set(float64(met.BlockSize))
		m.migratorTotalBlocks.WithLabelValues(name).Set(float64(met.TotalBlocks))
		m.migratorMigratedBlocks.WithLabelValues(name).Set(float64(met.MigratedBlocks))
		m.migratorReadyBlocks.WithLabelValues(name).Set(float64(met.ReadyBlocks))
		m.migratorActiveBlocks.WithLabelValues(name).Set(float64(met.ActiveBlocks))
		m.migratorTotalMigratedBlocks.WithLabelValues(name).Set(float64(met.TotalMigratedBlocks))
	})
}

func (m *Metrics) RemoveMigrator(name string) {
	m.remove(promSubMigrator, name)
}

func (m *Metrics) AddProtocol(name string, proto *protocol.RW) {
	m.add(promSubProtocol, name, protocolTick, func() {
		met := proto.GetMetrics()
		m.protocolPacketsSent.WithLabelValues(name).Set(float64(met.PacketsSent))
		m.protocolDataSent.WithLabelValues(name).Set(float64(met.DataSent))
		m.protocolPacketsRecv.WithLabelValues(name).Set(float64(met.PacketsRecv))
		m.protocolDataRecv.WithLabelValues(name).Set(float64(met.DataRecv))
		m.protocolWrites.WithLabelValues(name).Set(float64(met.Writes))
		m.protocolWriteErrors.WithLabelValues(name).Set(float64(met.WriteErrors))
		m.protocolWaitingForId.WithLabelValues(name).Set(float64(met.WaitingForID))
	})
}

func (m *Metrics) RemoveProtocol(name string) {
	m.remove(promSubProtocol, name)
}

func (m *Metrics) AddToProtocol(name string, proto *protocol.ToProtocol) {
	m.add(promSubToProtocol, name, toProtocolTick, func() {
		met := proto.GetMetrics()

		m.toProtocolSentEvents.WithLabelValues(name).Set(float64(met.SentEvents))
		m.toProtocolSentAltSources.WithLabelValues(name).Set(float64(met.SentAltSources))
		m.toProtocolSentHashes.WithLabelValues(name).Set(float64(met.SentHashes))
		m.toProtocolSentDevInfo.WithLabelValues(name).Set(float64(met.SentDevInfo))
		m.toProtocolSentDirtyList.WithLabelValues(name).Set(float64(met.SentDirtyList))
		m.toProtocolSentReadAt.WithLabelValues(name).Set(float64(met.SentReadAt))
		m.toProtocolSentWriteAtHash.WithLabelValues(name).Set(float64(met.SentWriteAtHash))
		m.toProtocolSentWriteAtHashBytes.WithLabelValues(name).Set(float64(met.SentWriteAtHashBytes))
		m.toProtocolSentWriteAtComp.WithLabelValues(name).Set(float64(met.SentWriteAtComp))
		m.toProtocolSentWriteAtCompBytes.WithLabelValues(name).Set(float64(met.SentWriteAtCompBytes))
		m.toProtocolSentWriteAtCompDataBytes.WithLabelValues(name).Set(float64(met.SentWriteAtCompDataBytes))
		m.toProtocolSentWriteAt.WithLabelValues(name).Set(float64(met.SentWriteAt))
		m.toProtocolSentWriteAtBytes.WithLabelValues(name).Set(float64(met.SentWriteAtBytes))
		m.toProtocolSentWriteAtWithMap.WithLabelValues(name).Set(float64(met.SentWriteAtWithMap))
		m.toProtocolSentRemoveFromMap.WithLabelValues(name).Set(float64(met.SentRemoveFromMap))
		m.toProtocolRecvNeedAt.WithLabelValues(name).Set(float64(met.RecvNeedAt))
		m.toProtocolRecvDontNeedAt.WithLabelValues(name).Set(float64(met.RecvDontNeedAt))
	})
}

func (m *Metrics) RemoveToProtocol(name string) {
	m.remove(promSubToProtocol, name)
}

func (m *Metrics) AddFromProtocol(name string, proto *protocol.FromProtocol) {
	m.add(promSubFromProtocol, name, fromProtocolTick, func() {
		met := proto.GetMetrics()

		m.fromProtocolRecvEvents.WithLabelValues(name).Set(float64(met.RecvEvents))
		m.fromProtocolRecvHashes.WithLabelValues(name).Set(float64(met.RecvHashes))
		m.fromProtocolRecvDevInfo.WithLabelValues(name).Set(float64(met.RecvDevInfo))
		m.fromProtocolRecvAltSources.WithLabelValues(name).Set(float64(met.RecvAltSources))
		m.fromProtocolRecvReadAt.WithLabelValues(name).Set(float64(met.RecvReadAt))
		m.fromProtocolRecvWriteAtHash.WithLabelValues(name).Set(float64(met.RecvWriteAtHash))
		m.fromProtocolRecvWriteAtComp.WithLabelValues(name).Set(float64(met.RecvWriteAtComp))
		m.fromProtocolRecvWriteAt.WithLabelValues(name).Set(float64(met.RecvWriteAt))
		m.fromProtocolRecvWriteAtWithMap.WithLabelValues(name).Set(float64(met.RecvWriteAtWithMap))
		m.fromProtocolRecvRemoveFromMap.WithLabelValues(name).Set(float64(met.RecvRemoveFromMap))
		m.fromProtocolRecvRemoveDev.WithLabelValues(name).Set(float64(met.RecvRemoveDev))
		m.fromProtocolRecvDirtyList.WithLabelValues(name).Set(float64(met.RecvDirtyList))
		m.fromProtocolSentNeedAt.WithLabelValues(name).Set(float64(met.SentNeedAt))
		m.fromProtocolSentDontNeedAt.WithLabelValues(name).Set(float64(met.SentDontNeedAt))
	})
}

func (m *Metrics) RemoveFromProtocol(name string) {
	m.remove(promSubFromProtocol, name)
}

func (m *Metrics) AddS3Storage(name string, s3 *sources.S3Storage) {
	m.add(promSubS3, name, s3Tick, func() {
		met := s3.Metrics()
		m.s3BlocksW.WithLabelValues(name).Set(float64(met.BlocksWCount))
		m.s3BlocksWBytes.WithLabelValues(name).Set(float64(met.BlocksWBytes))
		m.s3BlocksR.WithLabelValues(name).Set(float64(met.BlocksRCount))
		m.s3BlocksRBytes.WithLabelValues(name).Set(float64(met.BlocksRBytes))
	})

}

func (m *Metrics) RemoveS3Storage(name string) {
	m.remove(promSubS3, name)
}

func (m *Metrics) AddDirtyTracker(name string, dt *dirtytracker.Remote) {
	m.add(promSubDirtyTracker, name, dirtyTrackerTick, func() {
		met := dt.GetMetrics()
		m.dirtyTrackerBlockSize.WithLabelValues(name).Set(float64(met.BlockSize))
		m.dirtyTrackerTrackingBlocks.WithLabelValues(name).Set(float64(met.TrackingBlocks))
		m.dirtyTrackerDirtyBlocks.WithLabelValues(name).Set(float64(met.DirtyBlocks))
		m.dirtyTrackerMaxAgeDirtyMS.WithLabelValues(name).Set(float64(met.MaxAgeDirty))
	})
}

func (m *Metrics) RemoveDirtyTracker(name string) {
	m.remove(promSubDirtyTracker, name)
}

func (m *Metrics) AddVolatilityMonitor(name string, vm *volatilitymonitor.VolatilityMonitor) {
	m.add(promSubVolatilityMonitor, name, volatilityMonitorTick, func() {
		met := vm.GetMetrics()
		m.volatilityMonitorBlockSize.WithLabelValues(name).Set(float64(met.BlockSize))
		m.volatilityMonitorAvailable.WithLabelValues(name).Set(float64(met.Available))
		m.volatilityMonitorVolatility.WithLabelValues(name).Set(float64(met.Volatility))
	})
}

func (m *Metrics) RemoveVolatilityMonitor(name string) {
	m.remove(promSubVolatilityMonitor, name)
}

func (m *Metrics) AddMetrics(name string, mm *modules.Metrics) {
	m.add(promSubMetrics, name, metricsTick, func() {
		met := mm.GetMetrics()
		m.metricsReadOps.WithLabelValues(name).Set(float64(met.ReadOps))
		m.metricsReadBytes.WithLabelValues(name).Set(float64(met.ReadBytes))
		m.metricsReadErrors.WithLabelValues(name).Set(float64(met.ReadErrors))
		m.metricsReadTime.WithLabelValues(name).Set(float64(met.ReadTime))
		m.metricsWriteOps.WithLabelValues(name).Set(float64(met.WriteOps))
		m.metricsWriteBytes.WithLabelValues(name).Set(float64(met.WriteBytes))
		m.metricsWriteErrors.WithLabelValues(name).Set(float64(met.WriteErrors))
		m.metricsWriteTime.WithLabelValues(name).Set(float64(met.WriteTime))
		m.metricsFlushOps.WithLabelValues(name).Set(float64(met.FlushOps))
		m.metricsFlushErrors.WithLabelValues(name).Set(float64(met.FlushErrors))
		m.metricsFlushTime.WithLabelValues(name).Set(float64(met.FlushTime))
	})
}

func (m *Metrics) RemoveMetrics(name string) {
	m.remove(promSubMetrics, name)
}

func (m *Metrics) AddNBD(name string, mm *expose.ExposedStorageNBDNL) {
	m.add(promSubNbd, name, nbdTick, func() {
		met := mm.GetMetrics()
		m.nbdPacketsIn.WithLabelValues(name).Set(float64(met.PacketsIn))
		m.nbdPacketsOut.WithLabelValues(name).Set(float64(met.PacketsOut))
		m.nbdReadAt.WithLabelValues(name).Set(float64(met.ReadAt))
		m.nbdReadAtBytes.WithLabelValues(name).Set(float64(met.ReadAtBytes))
		m.nbdWriteAt.WithLabelValues(name).Set(float64(met.WriteAt))
		m.nbdWriteAtBytes.WithLabelValues(name).Set(float64(met.WriteAtBytes))
	})
}

func (m *Metrics) RemoveNBD(name string) {
	m.remove(promSubNbd, name)
}

func (m *Metrics) AddWaitingCache(name string, wc *waitingcache.Remote) {
	m.add(promSubWaitingCache, name, waitingCacheTick, func() {
		met := wc.GetMetrics()
		m.waitingCacheWaitForBlock.WithLabelValues(name).Set(float64(met.WaitForBlock))
		m.waitingCacheWaitForBlockHadRemote.WithLabelValues(name).Set(float64(met.WaitForBlockHadRemote))
		m.waitingCacheWaitForBlockHadLocal.WithLabelValues(name).Set(float64(met.WaitForBlockHadLocal))
		m.waitingCacheWaitForBlockLock.WithLabelValues(name).Set(float64(met.WaitForBlockLock))
		m.waitingCacheWaitForBlockLockDone.WithLabelValues(name).Set(float64(met.WaitForBlockLockDone))
		m.waitingCacheMarkAvailableLocalBlock.WithLabelValues(name).Set(float64(met.MarkAvailableLocalBlock))
		m.waitingCacheMarkAvailableRemoteBlock.WithLabelValues(name).Set(float64(met.MarkAvailableRemoteBlock))
		m.waitingCacheAvailableLocal.WithLabelValues(name).Set(float64(met.AvailableLocal))
		m.waitingCacheAvailableRemote.WithLabelValues(name).Set(float64(met.AvailableRemote))
	})
}

func (m *Metrics) RemoveWaitingCache(name string) {
	m.remove(promSubWaitingCache, name)
}
