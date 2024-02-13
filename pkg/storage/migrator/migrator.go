package migrator

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type MigratorConfig struct {
}

type Migrator struct {
	src_track           storage.TrackingStorageProvider // Tracks writes so we know which are dirty
	dest                storage.StorageProvider
	src_lock_fn         func()
	src_unlock_fn       func()
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
	block_size int,
	lock_fn func(),
	unlock_fn func(),
	block_order storage.BlockOrder) (*Migrator, error) {

	// TODO: Pass in configuration...
	concurrency_by_block := map[int]int{
		storage.BlockTypeAny:      32,
		storage.BlockTypeStandard: 32,
		storage.BlockTypeDirty:    100,
		storage.BlockTypePriority: 16,
	}

	num_blocks := (int(source.Size()) + block_size - 1) / block_size
	m := &Migrator{
		dest:                dest,
		src_track:           source,
		src_lock_fn:         lock_fn,
		src_unlock_fn:       unlock_fn,
		block_size:          block_size,
		num_blocks:          num_blocks,
		metric_moved_blocks: 0,
		block_order:         block_order,
		migrated_blocks:     util.NewBitfield(num_blocks),
		clean_blocks:        util.NewBitfield(num_blocks),
		concurrency:         make(map[int]chan bool),
	}

	if m.dest.Size() != m.src_track.Size() {
		return nil, errors.New("source and destination sizes must be equal for migration.")
	}

	// Initialize concurrency channels
	for b, v := range concurrency_by_block {
		m.concurrency[b] = make(chan bool, v)
	}
	return m, nil
}

/**
 * Migrate all storage to dest.
 */
func (m *Migrator) Migrate() error {
	m.ctime = time.Now()

	for {
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
				// If there was an error, we'll simply add it back to the block_order to be retried later for now.
				m.block_order.Add(block_no.Block)
				// TODO: Allow other options for error handling
				fmt.Printf("ERROR moving block %v\n", err)
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

		go func(block_no *storage.BlockInfo) {
			err := m.migrateBlock(block_no.Block)
			if err != nil {
				// TODO: Collect errors properly. Abort? Retry? fail?
				fmt.Printf("ERROR moving block %v\n", err)
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
	return nil
}

func (m *Migrator) WaitForCompletion() error {
	m.wg.Wait()
	return nil
}

/**
 * Show progress...
 *
 */
func (m *Migrator) ShowProgress() {
	migrated := m.migrated_blocks.Count(0, uint(m.num_blocks))
	perc_mig := float64(migrated*100) / float64(m.num_blocks)

	completed := m.clean_blocks.Count(0, uint(m.num_blocks))
	perc_complete := float64(completed*100) / float64(m.num_blocks)
	is_complete := ""
	if completed == m.num_blocks {
		is_complete = " COMPLETE"
	}
	moved := atomic.LoadInt64(&m.metric_moved_blocks)
	fmt.Printf("Migration %dms Migrated (%d/%d) [%.2f%%] Clean (%d/%d) [%.2f%%] %d blocks moved %s\n",
		time.Since(m.ctime).Milliseconds(),
		migrated,
		m.num_blocks,
		perc_mig,
		completed,
		m.num_blocks,
		perc_complete,
		moved,
		is_complete)
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
	if n != m.block_size || err != nil {
		return err
	}

	// Now write it to destStorage
	n, err = m.dest.WriteAt(buff, int64(offset))
	if n != m.block_size || err != nil {
		return err
	}

	atomic.AddInt64(&m.metric_moved_blocks, 1)
	// Mark it as done
	m.migrated_blocks.SetBit(block)
	m.clean_blocks.SetBit(block)
	return nil
}
