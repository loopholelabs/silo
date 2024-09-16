package migrator

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/integrity"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type MigratorConfig struct {
	Block_size       int
	Locker_handler   func()
	Unlocker_handler func()
	Error_handler    func(b *storage.BlockInfo, err error)
	Progress_handler func(p *MigrationProgress)
	Concurrency      map[int]int
	Integrity        bool
	Cancel_writes    bool
	Dedupe_writes    bool
	Recent_write_age time.Duration
}

func NewMigratorConfig() *MigratorConfig {
	return &MigratorConfig{
		Block_size:       0,
		Locker_handler:   func() {},
		Unlocker_handler: func() {},
		Error_handler:    func(b *storage.BlockInfo, err error) {},
		Progress_handler: func(p *MigrationProgress) {},
		Concurrency: map[int]int{
			storage.BlockTypeAny:      32,
			storage.BlockTypeStandard: 32,
			storage.BlockTypeDirty:    100,
			storage.BlockTypePriority: 16,
		},
		Integrity:        false,
		Cancel_writes:    false,
		Dedupe_writes:    false,
		Recent_write_age: time.Minute,
	}
}

func (mc *MigratorConfig) WithBlockSize(bs int) *MigratorConfig {
	mc.Block_size = bs
	return mc
}

type MigrationProgress struct {
	Total_blocks            int // Total blocks
	Migrated_blocks         int // Number of blocks that have been migrated
	Migrated_blocks_perc    float64
	Ready_blocks            int // Number of blocks which are up to date (clean). May go down as well as up.
	Ready_blocks_perc       float64
	Active_blocks           int // Number of blocks in progress now
	Total_Canceled_blocks   int // Total blocks that were cancelled
	Total_Migrated_blocks   int // Total blocks that were migrated
	Total_Duplicated_blocks int
}

type Migrator struct {
	source_tracker           storage.TrackingStorageProvider // Tracks writes so we know which are dirty
	source_mapped            *modules.MappedStorage
	dest_write_with_map      func([]byte, int64, map[uint64]uint64) (int, error)
	dest                     storage.StorageProvider
	source_lock_fn           func()
	source_unlock_fn         func()
	error_fn                 func(block *storage.BlockInfo, err error)
	progress_fn              func(*MigrationProgress)
	progress_lock            sync.Mutex
	progress_last            time.Time
	progress_last_status     *MigrationProgress
	block_size               int
	num_blocks               int
	metric_blocks_migrated   int64
	metric_blocks_canceled   int64
	metric_blocks_duplicates int64
	block_locks              []sync.Mutex
	moving_blocks            *util.Bitfield
	migrated_blocks          *util.Bitfield
	clean_blocks             *util.Bitfield
	waiting_blocks           *util.Bitfield
	last_written_blocks      []time.Time
	block_order              storage.BlockOrder
	ctime                    time.Time
	concurrency              map[int]chan bool
	wg                       sync.WaitGroup
	integrity                *integrity.IntegrityChecker
	cancel_writes            bool
	dedupe_writes            bool
	recent_write_age         time.Duration
}

func NewMigrator(source storage.TrackingStorageProvider,
	dest storage.StorageProvider,
	block_order storage.BlockOrder,
	config *MigratorConfig) (*Migrator, error) {

	num_blocks := (int(source.Size()) + config.Block_size - 1) / config.Block_size
	m := &Migrator{
		dest:                     dest,
		source_tracker:           source,
		source_lock_fn:           config.Locker_handler,
		source_unlock_fn:         config.Unlocker_handler,
		error_fn:                 config.Error_handler,
		progress_fn:              config.Progress_handler,
		block_size:               config.Block_size,
		num_blocks:               num_blocks,
		metric_blocks_migrated:   0,
		metric_blocks_canceled:   0,
		metric_blocks_duplicates: 0,
		block_order:              block_order,
		moving_blocks:            util.NewBitfield(num_blocks),
		migrated_blocks:          util.NewBitfield(num_blocks),
		clean_blocks:             util.NewBitfield(num_blocks),
		waiting_blocks:           util.NewBitfield(num_blocks),
		block_locks:              make([]sync.Mutex, num_blocks),
		concurrency:              make(map[int]chan bool),
		progress_last_status:     &MigrationProgress{},
		last_written_blocks:      make([]time.Time, num_blocks),
		recent_write_age:         config.Recent_write_age,
		cancel_writes:            config.Cancel_writes,
		dedupe_writes:            config.Dedupe_writes,
	}

	if m.dest.Size() != m.source_tracker.Size() {
		return nil, errors.New("source and destination sizes must be equal for migration.")
	}

	// Initialize concurrency channels
	for b, v := range config.Concurrency {
		m.concurrency[b] = make(chan bool, v)
	}

	if config.Integrity {
		m.integrity = integrity.NewIntegrityChecker(int64(m.dest.Size()), m.block_size)
	}
	return m, nil
}

/**
 * Set a MappedStorage for the source.
 *
 */
func (m *Migrator) SetSourceMapped(ms *modules.MappedStorage, writer func([]byte, int64, map[uint64]uint64) (int, error)) {
	m.source_mapped = ms
	m.dest_write_with_map = writer
}

/**
 * Set a block to already migrated state
 */
func (m *Migrator) SetMigratedBlock(block int) {
	m.block_locks[block].Lock()
	defer m.block_locks[block].Unlock()
	// Mark it as done
	m.migrated_blocks.SetBit(block)
	m.clean_blocks.SetBit(block)

	m.last_written_blocks[block] = time.Now()

	m.reportProgress(false)
}

/**
 * Migrate storage to dest.
 */
func (m *Migrator) Migrate(num_blocks int) error {
	m.ctime = time.Now()

	for b := 0; b < num_blocks; b++ {
		i := m.block_order.GetNext()
		if i == storage.BlockInfoFinish {
			break
		}

		cc, ok := m.concurrency[i.Type]
		if !ok {
			cc = m.concurrency[storage.BlockTypeAny]
		}
		cc <- true

		m.wg.Add(1)

		go func(block_no *storage.BlockInfo) {
			err := m.migrateBlock(block_no.Block)
			if err != nil {
				m.error_fn(block_no, err)
			}

			m.wg.Done()
			cc, ok := m.concurrency[block_no.Type]
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
		blocks := m.source_tracker.Sync()
		return blocks.Collect(0, blocks.Length())
	}
	return m.GetLatestDirtyFunc(getter)
}

/**
 * Get the latest dirty blocks.
 * If there a no more dirty blocks, we leave the src locked.
 */
func (m *Migrator) GetLatestDirtyFunc(getter func() []uint) []uint {
	m.source_lock_fn()

	block_nos := getter()
	if len(block_nos) != 0 {
		m.source_unlock_fn()
		return block_nos
	}
	return nil
}

func (m *Migrator) Unlock() {
	m.source_unlock_fn()
}

/**
 * MigrateDirty migrates a list of dirty blocks.
 * An attempt is made to cancel any existing writes for the blocks first.
 */
func (m *Migrator) MigrateDirty(blocks []uint) error {
	for _, pos := range blocks {
		i := &storage.BlockInfo{Block: int(pos), Type: storage.BlockTypeDirty}

		// Check if there has been a successful write to the block recently
		// If there has, then cancel any in progress write
		if m.cancel_writes && time.Since(m.last_written_blocks[pos]) < m.recent_write_age {
			m.dest.CancelWrites(int64(pos*uint(m.block_size)), int64(m.block_size))
		}

		one_waiting := m.waiting_blocks.SetBitIfClear(int(pos))

		// If there's already one waiting here for this block, we want to do nothing.
		if m.dedupe_writes && one_waiting {
			// Someone else is already waiting here. We will quit.
			atomic.AddInt64(&m.metric_blocks_duplicates, 1)
			return nil
		}

		cc, ok := m.concurrency[i.Type]
		if !ok {
			cc = m.concurrency[storage.BlockTypeAny]
		}
		cc <- true

		m.waiting_blocks.ClearBit(int(pos)) // Allow more writes for this to come in now.

		m.wg.Add(1)

		m.clean_blocks.ClearBit(int(pos))

		go func(block_no *storage.BlockInfo) {
			err := m.migrateBlock(block_no.Block)
			if err != nil {
				m.error_fn(block_no, err)
			}

			m.wg.Done()
			cc, ok := m.concurrency[block_no.Type]
			if !ok {
				cc = m.concurrency[storage.BlockTypeAny]
			}
			<-cc
		}(i)

	}
	return nil
}

func (m *Migrator) WaitForCompletion() error {
	m.wg.Wait()
	m.reportProgress(true) // Force progress_fn callback to be called
	return nil
}

func (m *Migrator) GetHashes() map[uint][sha256.Size]byte {
	if m.integrity == nil {
		return nil
	}
	return m.integrity.GetHashes()
}

func (m *Migrator) reportProgress(forced bool) {
	m.progress_lock.Lock()
	defer m.progress_lock.Unlock()

	if !forced && time.Since(m.progress_last).Milliseconds() < 100 {
		return // Rate limit progress to once per 100ms
	}

	migrated := m.migrated_blocks.Count(0, uint(m.num_blocks))
	perc_mig := float64(migrated*100) / float64(m.num_blocks)

	completed := m.clean_blocks.Count(0, uint(m.num_blocks))
	perc_complete := float64(completed*100) / float64(m.num_blocks)

	if completed == m.progress_last_status.Ready_blocks &&
		migrated == m.progress_last_status.Migrated_blocks {
		return // Nothing has really changed
	}

	m.progress_last = time.Now()
	m.progress_last_status = &MigrationProgress{
		Total_blocks:            m.num_blocks,
		Migrated_blocks:         migrated,
		Migrated_blocks_perc:    perc_mig,
		Ready_blocks:            completed,
		Ready_blocks_perc:       perc_complete,
		Active_blocks:           m.moving_blocks.Count(0, uint(m.num_blocks)),
		Total_Canceled_blocks:   int(atomic.LoadInt64(&m.metric_blocks_canceled)),
		Total_Migrated_blocks:   int(atomic.LoadInt64(&m.metric_blocks_migrated)),
		Total_Duplicated_blocks: int(atomic.LoadInt64(&m.metric_blocks_duplicates)),
	}
	// Callback
	m.progress_fn(m.progress_last_status)
}

/**
 * Get overall status of the migration
 *
 */
func (m *Migrator) Status() *MigrationProgress {
	m.progress_lock.Lock()
	defer m.progress_lock.Unlock()

	migrated := m.migrated_blocks.Count(0, uint(m.num_blocks))
	perc_mig := float64(migrated*100) / float64(m.num_blocks)

	completed := m.clean_blocks.Count(0, uint(m.num_blocks))
	perc_complete := float64(completed*100) / float64(m.num_blocks)

	return &MigrationProgress{
		Total_blocks:            m.num_blocks,
		Migrated_blocks:         migrated,
		Migrated_blocks_perc:    perc_mig,
		Ready_blocks:            completed,
		Ready_blocks_perc:       perc_complete,
		Active_blocks:           m.moving_blocks.Count(0, uint(m.num_blocks)),
		Total_Canceled_blocks:   int(atomic.LoadInt64(&m.metric_blocks_canceled)),
		Total_Migrated_blocks:   int(atomic.LoadInt64(&m.metric_blocks_migrated)),
		Total_Duplicated_blocks: int(atomic.LoadInt64(&m.metric_blocks_duplicates)),
	}
}

/**
 * Migrate a single block to dest
 *
 */
func (m *Migrator) migrateBlock(block int) error {
	m.block_locks[block].Lock()
	defer m.block_locks[block].Unlock()

	m.moving_blocks.SetBit(block)
	defer m.moving_blocks.ClearBit(block)

	// TODO: Pool these somewhere...
	buff := make([]byte, m.block_size)
	offset := int(block) * m.block_size
	// Read from source
	n, err := m.source_tracker.ReadAt(buff, int64(offset))
	if err != nil {
		return err
	}

	var idmap map[uint64]uint64
	if m.source_mapped != nil {
		idmap = m.source_mapped.GetMapForSourceRange(int64(offset), m.block_size)
	}

	// TODO: Need to figure out how we want to route the WriteAtWithMap here...

	// If it was a partial read, truncate
	buff = buff[:n]

	if m.source_mapped != nil {
		n, err = m.dest_write_with_map(buff, int64(offset), idmap)
	} else {
		// Now write it to destStorage
		n, err = m.dest.WriteAt(buff, int64(offset))
	}
	if n != len(buff) || err != nil {
		if errors.Is(err, context.Canceled) {
			atomic.AddInt64(&m.metric_blocks_canceled, 1)
		}
		return err
	}

	// Set the last successful write for this block
	m.last_written_blocks[block] = time.Now()

	// If we have an integrity check setup, lets hash the block for later...
	if m.integrity != nil {
		m.integrity.HashBlock(uint(block), buff)
	}

	atomic.AddInt64(&m.metric_blocks_migrated, 1)
	// Mark it as done
	m.migrated_blocks.SetBit(block)
	m.clean_blocks.SetBit(block)

	m.reportProgress(false)
	return nil
}
