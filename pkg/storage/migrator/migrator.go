package migrator

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/integrity"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type Config struct {
	Logger          types.RootLogger
	BlockSize       int
	LockerHandler   func()
	UnlockerHandler func()
	ErrorHandler    func(b *storage.BlockInfo, err error)
	ProgressHandler func(p *MigrationProgress)
	BlockHandler    func(b *storage.BlockInfo, id uint64, block []byte)
	Concurrency     map[int]int
	Integrity       bool
	CancelWrites    bool
	DedupeWrites    bool
	RecentWriteAge  time.Duration
}

func NewConfig() *Config {
	return &Config{
		Logger:          nil,
		BlockSize:       0,
		LockerHandler:   func() {},
		UnlockerHandler: func() {},
		ErrorHandler:    func(_ *storage.BlockInfo, _ error) {},
		ProgressHandler: func(_ *MigrationProgress) {},
		BlockHandler:    func(_ *storage.BlockInfo, _ uint64, _ []byte) {},
		Concurrency: map[int]int{
			storage.BlockTypeAny:      32,
			storage.BlockTypeStandard: 32,
			storage.BlockTypeDirty:    100,
			storage.BlockTypePriority: 16,
		},
		Integrity:      false,
		CancelWrites:   false,
		DedupeWrites:   false,
		RecentWriteAge: time.Minute,
	}
}

func (mc *Config) WithBlockSize(bs int) *Config {
	mc.BlockSize = bs
	return mc
}

type MigrationProgress struct {
	BlockSize             int
	TotalBlocks           int // Total blocks
	MigratedBlocks        int // Number of blocks that have been migrated
	MigratedBlocksPerc    float64
	ReadyBlocks           int // Number of blocks which are up to date (clean). May go down as well as up.
	ReadyBlocksPerc       float64
	ActiveBlocks          int // Number of blocks in progress now
	TotalCanceledBlocks   int // Total blocks that were cancelled
	TotalMigratedBlocks   int // Total blocks that were migrated
	TotalDuplicatedBlocks int
}

type Migrator struct {
	uuid                   uuid.UUID
	logger                 types.RootLogger
	sourceTracker          storage.TrackingProvider // Tracks writes so we know which are dirty
	sourceMapped           *modules.MappedStorage
	destWriteWithMap       func([]byte, int64, map[uint64]uint64) (int, error)
	dest                   storage.Provider
	sourceLockFn           func()
	sourceUnlockFn         func()
	errorFn                func(block *storage.BlockInfo, err error)
	progressFn             func(*MigrationProgress)
	blockFn                func(block *storage.BlockInfo, id uint64, data []byte)
	progressLock           sync.Mutex
	progressLast           time.Time
	progressLastStatus     *MigrationProgress
	blockSize              int
	numBlocks              int
	metricBlocksMigrated   int64
	metricBlocksCanceled   int64
	metricBlocksDuplicates int64
	blockLocks             []sync.Mutex
	movingBlocks           *util.Bitfield
	migratedBlocks         *util.Bitfield
	cleanBlocks            *util.Bitfield
	waitingBlocks          *util.Bitfield
	lastWrittenBlocks      []time.Time
	blockOrder             storage.BlockOrder
	ctime                  time.Time
	concurrency            map[int]chan bool
	wg                     sync.WaitGroup
	integrity              *integrity.Checker
	cancelWrites           bool
	dedupeWrites           bool
	recentWriteAge         time.Duration
	migrationStarted       bool
	migrationStartTime     time.Time
}

func NewMigrator(source storage.TrackingProvider,
	dest storage.Provider,
	blockOrder storage.BlockOrder,
	config *Config) (*Migrator, error) {

	numBlocks := (int(source.Size()) + config.BlockSize - 1) / config.BlockSize
	m := &Migrator{
		uuid:                   uuid.New(),
		logger:                 config.Logger,
		migrationStarted:       false,
		migrationStartTime:     time.Now(),
		dest:                   dest,
		sourceTracker:          source,
		sourceLockFn:           config.LockerHandler,
		sourceUnlockFn:         config.UnlockerHandler,
		errorFn:                config.ErrorHandler,
		progressFn:             config.ProgressHandler,
		blockFn:                config.BlockHandler,
		blockSize:              config.BlockSize,
		numBlocks:              numBlocks,
		metricBlocksMigrated:   0,
		metricBlocksCanceled:   0,
		metricBlocksDuplicates: 0,
		blockOrder:             blockOrder,
		movingBlocks:           util.NewBitfield(numBlocks),
		migratedBlocks:         util.NewBitfield(numBlocks),
		cleanBlocks:            util.NewBitfield(numBlocks),
		waitingBlocks:          util.NewBitfield(numBlocks),
		blockLocks:             make([]sync.Mutex, numBlocks),
		concurrency:            make(map[int]chan bool),
		progressLastStatus:     &MigrationProgress{},
		lastWrittenBlocks:      make([]time.Time, numBlocks),
		recentWriteAge:         config.RecentWriteAge,
		cancelWrites:           config.CancelWrites,
		dedupeWrites:           config.DedupeWrites,
	}

	if m.dest.Size() != m.sourceTracker.Size() {
		return nil, errors.New("source and destination sizes must be equal for migration")
	}

	// Initialize concurrency channels
	for b, v := range config.Concurrency {
		m.concurrency[b] = make(chan bool, v)
	}

	if config.Integrity {
		m.integrity = integrity.NewChecker(int64(m.dest.Size()), m.blockSize)
	}

	if m.logger != nil {
		m.logger.Debug().
			Str("uuid", m.uuid.String()).
			Uint64("size", source.Size()).
			Msg("Created migrator")
	}

	return m, nil
}

/**
 * Set a MappedStorage for the source.
 *
 */
func (m *Migrator) SetSourceMapped(ms *modules.MappedStorage, writer func([]byte, int64, map[uint64]uint64) (int, error)) {
	m.sourceMapped = ms
	m.destWriteWithMap = writer
}

/**
 * Set a block to already migrated state
 */
func (m *Migrator) SetMigratedBlock(block int) {
	m.blockLocks[block].Lock()
	defer m.blockLocks[block].Unlock()
	// Mark it as done
	m.migratedBlocks.SetBit(block)
	m.cleanBlocks.SetBit(block)

	m.lastWrittenBlocks[block] = time.Now()

	m.reportProgress(false)
}

func (m *Migrator) startMigration() {
	if m.migrationStarted {
		return
	}
	m.migrationStarted = true
	m.migrationStartTime = time.Now()

	if m.logger != nil {
		m.logger.Debug().
			Str("uuid", m.uuid.String()).
			Uint64("size", m.sourceTracker.Size()).
			Msg("Migration started")
	}

	// Tell the source to stop sync, and send alternateSources to the destination.
	as := storage.SendSiloEvent(m.sourceTracker, "sync.stop", nil)
	if len(as) == 1 {
		storage.SendSiloEvent(m.dest, "sources", as[0])
		if m.logger != nil {
			m.logger.Debug().
				Str("uuid", m.uuid.String()).
				Uint64("size", m.sourceTracker.Size()).
				Int("sources", len(as[0].([]packets.AlternateSource))).
				Msg("Migrator alternate sources sent to destination")
		}
	}
}

/**
 * Migrate storage to dest.
 */
func (m *Migrator) Migrate(numBlocks int) error {
	if m.logger != nil {
		m.logger.Debug().
			Str("uuid", m.uuid.String()).
			Uint64("size", m.sourceTracker.Size()).
			Int("blocks", numBlocks).
			Msg("Migrate")
	}

	m.startMigration()
	m.ctime = time.Now()

	for b := 0; b < numBlocks; b++ {
		i := m.blockOrder.GetNext()
		if i == storage.BlockInfoFinish {
			break
		}

		cc, ok := m.concurrency[i.Type]
		if !ok {
			cc = m.concurrency[storage.BlockTypeAny]
		}
		cc <- true

		m.wg.Add(1)

		go func(blockNo *storage.BlockInfo) {
			data, err := m.migrateBlock(blockNo.Block)
			if err != nil {
				m.errorFn(blockNo, err)
			} else {
				m.blockFn(blockNo, 0, data)
			}

			m.wg.Done()
			cc, ok := m.concurrency[blockNo.Type]
			if !ok {
				cc = m.concurrency[storage.BlockTypeAny]
			}
			<-cc
		}(i)
	}
	return nil
}

/**
 * Get the latest dirty blocks.
 * If there a no more dirty blocks, we leave the src locked.
 */
func (m *Migrator) GetLatestDirty() []uint {
	getter := func() []uint {
		blocks := m.sourceTracker.Sync()
		return blocks.Collect(0, blocks.Length())
	}
	return m.GetLatestDirtyFunc(getter)
}

/**
 * Get the latest dirty blocks.
 * If there a no more dirty blocks, we leave the src locked.
 */
func (m *Migrator) GetLatestDirtyFunc(getter func() []uint) []uint {
	m.sourceLockFn()

	blockNos := getter()
	if len(blockNos) != 0 {
		m.sourceUnlockFn()
		return blockNos
	}
	return nil
}

func (m *Migrator) Unlock() {
	m.sourceUnlockFn()
}

/**
 * MigrateDirty migrates a list of dirty blocks.
 * An attempt is made to cancel any existing writes for the blocks first.
 */
func (m *Migrator) MigrateDirty(blocks []uint) error {
	return m.MigrateDirtyWithID(blocks, 0)
}

/**
 *
 * You can give a tracking ID which will turn up at block_fn on success
 */
func (m *Migrator) MigrateDirtyWithID(blocks []uint, tid uint64) error {
	if m.logger != nil {
		m.logger.Debug().
			Str("uuid", m.uuid.String()).
			Uint64("size", m.sourceTracker.Size()).
			Int("blocks", len(blocks)).
			Msg("Migrate dirty")
	}

	m.startMigration()

	for _, pos := range blocks {
		i := &storage.BlockInfo{Block: int(pos), Type: storage.BlockTypeDirty}

		// Check if there has been a successful write to the block recently
		// If there has, then cancel any in progress write
		if m.cancelWrites && time.Since(m.lastWrittenBlocks[pos]) < m.recentWriteAge {
			m.dest.CancelWrites(int64(pos*uint(m.blockSize)), int64(m.blockSize))
		}

		oneWaiting := m.waitingBlocks.SetBitIfClear(int(pos))

		// If there's already one waiting here for this block, we want to do nothing.
		if m.dedupeWrites && oneWaiting {
			// Someone else is already waiting here. We will quit.
			atomic.AddInt64(&m.metricBlocksDuplicates, 1)
			return nil
		}

		cc, ok := m.concurrency[i.Type]
		if !ok {
			cc = m.concurrency[storage.BlockTypeAny]
		}
		cc <- true

		m.waitingBlocks.ClearBit(int(pos)) // Allow more writes for this to come in now.

		m.wg.Add(1)

		m.cleanBlocks.ClearBit(int(pos))

		go func(blockNo *storage.BlockInfo, trackId uint64) {
			data, err := m.migrateBlock(blockNo.Block)
			if err != nil {
				m.errorFn(blockNo, err)
			} else {
				m.blockFn(blockNo, trackId, data)
			}

			m.wg.Done()
			cc, ok := m.concurrency[blockNo.Type]
			if !ok {
				cc = m.concurrency[storage.BlockTypeAny]
			}
			<-cc
		}(i, tid)

	}
	return nil
}

func (m *Migrator) WaitForCompletion() error {
	if m.logger != nil {
		m.logger.Debug().
			Str("uuid", m.uuid.String()).
			Uint64("size", m.sourceTracker.Size()).
			Msg("Migration waiting for completion")
	}
	m.wg.Wait()
	m.reportProgress(true)
	if m.logger != nil {
		m.logger.Debug().
			Str("uuid", m.uuid.String()).
			Uint64("size", m.sourceTracker.Size()).
			Int64("TimeMigratingMS", time.Since(m.migrationStartTime).Milliseconds()).
			Msg("Migration complete")
	}
	return nil
}

func (m *Migrator) GetHashes() map[uint][sha256.Size]byte {
	if m.integrity == nil {
		return nil
	}
	return m.integrity.GetHashes()
}

func (m *Migrator) reportProgress(forced bool) {
	m.progressLock.Lock()
	defer m.progressLock.Unlock()

	if !forced && time.Since(m.progressLast).Milliseconds() < 100 {
		return // Rate limit progress to once per 100ms
	}

	migrated := m.migratedBlocks.Count(0, uint(m.numBlocks))
	percMig := float64(migrated*100) / float64(m.numBlocks)

	completed := m.cleanBlocks.Count(0, uint(m.numBlocks))
	percComplete := float64(completed*100) / float64(m.numBlocks)

	if completed == m.progressLastStatus.ReadyBlocks &&
		migrated == m.progressLastStatus.MigratedBlocks {
		return // Nothing has really changed
	}

	m.progressLast = time.Now()
	m.progressLastStatus = &MigrationProgress{
		BlockSize:             m.blockSize,
		TotalBlocks:           m.numBlocks,
		MigratedBlocks:        migrated,
		MigratedBlocksPerc:    percMig,
		ReadyBlocks:           completed,
		ReadyBlocksPerc:       percComplete,
		ActiveBlocks:          m.movingBlocks.Count(0, uint(m.numBlocks)),
		TotalCanceledBlocks:   int(atomic.LoadInt64(&m.metricBlocksCanceled)),
		TotalMigratedBlocks:   int(atomic.LoadInt64(&m.metricBlocksMigrated)),
		TotalDuplicatedBlocks: int(atomic.LoadInt64(&m.metricBlocksDuplicates)),
	}

	if m.logger != nil {
		m.logger.Debug().
			Str("uuid", m.uuid.String()).
			Uint64("size", m.sourceTracker.Size()).
			Int("TotalBlocks", m.progressLastStatus.TotalBlocks).
			Int("MigratedBlocks", m.progressLastStatus.MigratedBlocks).
			Float64("MigratedBlocksPerc", m.progressLastStatus.MigratedBlocksPerc).
			Int("ReadyBlocks", m.progressLastStatus.ReadyBlocks).
			Float64("ReadyBlocksPerc", m.progressLastStatus.ReadyBlocksPerc).
			Int("ActiveBlocks", m.progressLastStatus.ActiveBlocks).
			Int("TotalCanceledBlocks", m.progressLastStatus.TotalCanceledBlocks).
			Int("TotalMigratedBlocks", m.progressLastStatus.TotalMigratedBlocks).
			Int("TotalDuplicatedBlocks", m.progressLastStatus.TotalDuplicatedBlocks).
			Int64("TimeMigratingMS", time.Since(m.migrationStartTime).Milliseconds()).
			Msg("Migration progress")
	}

	// Callback
	m.progressFn(m.progressLastStatus)
}

/**
 * Get overall status of the migration
 *
 */
func (m *Migrator) GetMetrics() *MigrationProgress {
	m.progressLock.Lock()
	defer m.progressLock.Unlock()

	migrated := m.migratedBlocks.Count(0, uint(m.numBlocks))
	percMig := float64(migrated*100) / float64(m.numBlocks)

	completed := m.cleanBlocks.Count(0, uint(m.numBlocks))
	percComplete := float64(completed*100) / float64(m.numBlocks)

	return &MigrationProgress{
		BlockSize:             m.blockSize,
		TotalBlocks:           m.numBlocks,
		MigratedBlocks:        migrated,
		MigratedBlocksPerc:    percMig,
		ReadyBlocks:           completed,
		ReadyBlocksPerc:       percComplete,
		ActiveBlocks:          m.movingBlocks.Count(0, uint(m.numBlocks)),
		TotalCanceledBlocks:   int(atomic.LoadInt64(&m.metricBlocksCanceled)),
		TotalMigratedBlocks:   int(atomic.LoadInt64(&m.metricBlocksMigrated)),
		TotalDuplicatedBlocks: int(atomic.LoadInt64(&m.metricBlocksDuplicates)),
	}
}

/**
 * Migrate a single block to dest
 *
 */
func (m *Migrator) migrateBlock(block int) ([]byte, error) {
	m.blockLocks[block].Lock()
	defer m.blockLocks[block].Unlock()

	m.movingBlocks.SetBit(block)
	defer m.movingBlocks.ClearBit(block)

	// TODO: Pool these somewhere...
	buff := make([]byte, m.blockSize)
	offset := block * m.blockSize
	// Read from source
	n, err := m.sourceTracker.ReadAt(buff, int64(offset))
	if err != nil {
		if m.logger != nil {
			m.logger.Error().
				Str("uuid", m.uuid.String()).
				Uint64("size", m.sourceTracker.Size()).
				Err(err).
				Msg("Migration error reading from source")
		}
		return nil, err
	}

	var idmap map[uint64]uint64
	if m.sourceMapped != nil {
		idmap = m.sourceMapped.GetMapForSourceRange(int64(offset), m.blockSize)
	}

	// If it was a partial read, truncate
	buff = buff[:n]

	if m.sourceMapped != nil {
		n, err = m.destWriteWithMap(buff, int64(offset), idmap)
	} else {
		// Now write it to destStorage
		n, err = m.dest.WriteAt(buff, int64(offset))
	}

	if n != len(buff) || err != nil {
		if errors.Is(err, context.Canceled) {
			atomic.AddInt64(&m.metricBlocksCanceled, 1)
		} else if m.logger != nil {
			m.logger.Error().
				Str("uuid", m.uuid.String()).
				Uint64("size", m.sourceTracker.Size()).
				Err(err).
				Msg("Migration error writing to destination")
		}
		return nil, err
	}

	// Set the last successful write for this block
	m.lastWrittenBlocks[block] = time.Now()

	// If we have an integrity check setup, lets hash the block for later...
	if m.integrity != nil {
		m.integrity.HashBlock(uint(block), buff)
	}

	atomic.AddInt64(&m.metricBlocksMigrated, 1)
	// Mark it as done
	m.migratedBlocks.SetBit(block)
	m.cleanBlocks.SetBit(block)

	m.reportProgress(false)
	return buff, nil
}
