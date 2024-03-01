package migrator

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type MigratorConfig struct {
	BlockSize       int
	LockerHandler   func()
	UnlockerHandler func()
	ErrorHandler    func(b *storage.BlockInfo, err error)
	ProgressHandler func(p *MigrationProgress)
	Concurrency     map[int]int
}

func NewMigratorConfig() *MigratorConfig {
	return &MigratorConfig{
		BlockSize:       0,
		LockerHandler:   func() {},
		UnlockerHandler: func() {},
		ErrorHandler:    func(b *storage.BlockInfo, err error) {},
		ProgressHandler: func(p *MigrationProgress) {},
		Concurrency: map[int]int{
			storage.BlockTypeAny:      32,
			storage.BlockTypeStandard: 32,
			storage.BlockTypeDirty:    100,
			storage.BlockTypePriority: 16,
		},
	}
}

func (mc *MigratorConfig) WithBlockSize(bs int) *MigratorConfig {
	mc.BlockSize = bs
	return mc
}

type MigrationProgress struct {
	TotalBlocks        int // Total blocks
	MigratedBlocks     int // Number of blocks that have been migrated
	MigratedBlocksPerc float64
	ReadyBlocks        int // Number of blocks which are up to date (clean). May go down as well as up.
	ReadyBlocksPerc    float64
	ActiveBlocks       int // Number of blocks in progress now
}

type Migrator struct {
	src_track           storage.TrackingStorageProvider // Tracks writes so we know which are dirty
	dest                storage.StorageProvider
	src_lock_fn         func()
	src_unlock_fn       func()
	error_fn            func(block *storage.BlockInfo, err error)
	progress_fn         func(*MigrationProgress)
	block_size          int
	num_blocks          int
	metric_moved_blocks int64
	moving_blocks       *util.Bitfield
	migrated_blocks     *util.Bitfield
	clean_blocks        *util.Bitfield
	block_order         storage.BlockOrder
	ctime               time.Time
	concurrency         map[int]chan bool
	wg                  sync.WaitGroup
}

func NewMigrator(source storage.TrackingStorageProvider,
	dest storage.StorageProvider,
	block_order storage.BlockOrder,
	config *MigratorConfig) (*Migrator, error) {

	num_blocks := (int(source.Size()) + config.BlockSize - 1) / config.BlockSize
	m := &Migrator{
		dest:                dest,
		src_track:           source,
		src_lock_fn:         config.LockerHandler,
		src_unlock_fn:       config.UnlockerHandler,
		error_fn:            config.ErrorHandler,
		progress_fn:         config.ProgressHandler,
		block_size:          config.BlockSize,
		num_blocks:          num_blocks,
		metric_moved_blocks: 0,
		block_order:         block_order,
		moving_blocks:       util.NewBitfield(num_blocks),
		migrated_blocks:     util.NewBitfield(num_blocks),
		clean_blocks:        util.NewBitfield(num_blocks),
		concurrency:         make(map[int]chan bool),
	}

	if m.dest.Size() != m.src_track.Size() {
		return nil, errors.New("source and destination sizes must be equal for migration.")
	}

	// Initialize concurrency channels
	for b, v := range config.Concurrency {
		m.concurrency[b] = make(chan bool, v)
	}
	return m, nil
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

		m.moving_blocks.SetBit(i.Block)
		go func(block_no *storage.BlockInfo) {
			err := m.migrateBlock(block_no.Block)
			m.moving_blocks.ClearBit(i.Block)
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
	// Queue up some dirty blocks
	m.src_lock_fn()

	// Check for any dirty blocks to be added on
	blocks := m.src_track.Sync()
	changed := blocks.Count(0, blocks.Length())
	if changed != 0 {
		m.src_unlock_fn()

		block_nos := blocks.Collect(0, blocks.Length())
		return block_nos
	}
	return nil
}

/**
 * MigrateDirty migrates a list of dirty blocks.
 */
func (m *Migrator) MigrateDirty(blocks []uint) error {
	for _, pos := range blocks {
		i := &storage.BlockInfo{Block: int(pos), Type: storage.BlockTypeDirty}

		cc, ok := m.concurrency[i.Type]
		if !ok {
			cc = m.concurrency[storage.BlockTypeAny]
		}
		cc <- true

		m.wg.Add(1)

		m.moving_blocks.SetBit(i.Block)
		go func(block_no *storage.BlockInfo) {
			err := m.migrateBlock(block_no.Block)
			m.moving_blocks.ClearBit(i.Block)
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

		m.clean_blocks.ClearBit(int(pos))
	}
	m.reportProgress()
	return nil
}

func (m *Migrator) WaitForCompletion() error {
	m.wg.Wait()
	return nil
}

func (m *Migrator) reportProgress() {
	migrated := m.migrated_blocks.Count(0, uint(m.num_blocks))
	perc_mig := float64(migrated*100) / float64(m.num_blocks)

	completed := m.clean_blocks.Count(0, uint(m.num_blocks))
	perc_complete := float64(completed*100) / float64(m.num_blocks)

	// Callback
	m.progress_fn(&MigrationProgress{
		TotalBlocks:        m.num_blocks,
		MigratedBlocks:     migrated,
		MigratedBlocksPerc: perc_mig,
		ReadyBlocks:        completed,
		ReadyBlocksPerc:    perc_complete,
		ActiveBlocks:       m.moving_blocks.Count(0, uint(m.num_blocks)),
	})
}

/**
 * Migrate a single block to dest
 *
 */
func (m *Migrator) migrateBlock(block int) error {
	buff := make([]byte, m.block_size)
	offset := int(block) * m.block_size
	// Read from source
	n, err := m.src_track.ReadAt(buff, int64(offset))
	if err != nil {
		return err
	}

	// If it was a partial read, truncate
	buff = buff[:n]

	// Now write it to destStorage
	n, err = m.dest.WriteAt(buff, int64(offset))
	if n != len(buff) || err != nil {
		return err
	}

	atomic.AddInt64(&m.metric_moved_blocks, 1)
	// Mark it as done
	m.migrated_blocks.SetBit(block)
	m.clean_blocks.SetBit(block)

	m.reportProgress()
	return nil
}
