package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/util"
)

type Migrator struct {
	src_track           TrackingStorageProvider
	dest                StorageProvider
	src_lock_fn         func()
	src_unlock_fn       func()
	block_size          int
	num_blocks          int
	metric_moved_blocks int64
	migrated_blocks     *util.Bitfield
	clean_blocks        *util.Bitfield
	block_order         BlockOrder
	ctime               time.Time

	// Our queue
	blocks_to_move   chan uint
	blocks_by_n      map[uint]bool
	blocks_by_n_lock sync.Mutex
}

func NewMigrator(source TrackingStorageProvider,
	dest StorageProvider,
	block_size int,
	lock_fn func(),
	unlock_fn func(),
	block_order BlockOrder) *Migrator {
	num_blocks := (int(source.Size()) + block_size - 1) / block_size
	return &Migrator{
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
		blocks_to_move:      make(chan uint, num_blocks),
		blocks_by_n:         make(map[uint]bool),
	}
}

func (m *Migrator) RequestBlock(block int) {
	// Request a block as priority...

	// Wait until it's completed
}

/**
 * Migrate storage to dest.
 * This naively continues until all data is synced.
 * It relies on being able to transfer a block quickly before the source changes something again.
 */
func (m *Migrator) Migrate() error {
	if m.dest.Size() != m.src_track.Size() {
		return errors.New("Sizes must be equal for migration")
	}

	m.ctime = time.Now()

	concurrency := make(chan bool, 1) // max concurrency
	var wg sync.WaitGroup

	for {
		// This will get the next queued migration, OR from the priority list
		i, done := m.getNextBlock()
		if done {
			break
		}

		// Start a migrator for the block...
		concurrency <- true
		wg.Add(1)

		go func(block_no uint) {
			err := m.migrateBlock(int(block_no))
			if err != nil {
				fmt.Printf("ERROR moving block %v\n", err)
			}

			m.showProgress()
			// TODO: If we have a notify for this block, let everyone know it's available

			fmt.Printf("Block moved %d\n", block_no)
			fmt.Printf("DATA %d,%d,,,,\n", time.Now().UnixMilli(), block_no)
			wg.Done()
			<-concurrency
		}(i)
	}

	fmt.Printf("Waiting for pending transfers...\n")
	wg.Wait()

	m.showProgress()

	return nil
}

/**
 * Show progress...
 *
 */
func (m *Migrator) showProgress() {
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

/**
 * Check for any dirty blocks, and add them to the queue
 *
 */
func (m *Migrator) queueDirtyBlocks() bool {
	m.src_lock_fn()

	// Check for any dirty blocks to be added on
	blocks := m.src_track.Sync()
	changed := blocks.Count(0, blocks.Length())
	if changed != 0 {
		m.src_unlock_fn()

		fmt.Printf("Got %d more dirty blocks...\n", changed)

		blocks.Exec(0, blocks.Length(), func(pos uint) bool {
			m.blocks_by_n_lock.Lock()
			_, ok := m.blocks_by_n[pos] // Dedup pending by block
			if !ok {
				m.blocks_to_move <- pos
				m.blocks_by_n[pos] = true
				m.clean_blocks.ClearBit(int(pos))
			}
			m.blocks_by_n_lock.Unlock()
			return true
		})
		m.showProgress()
		return true
	}
	return false
}

func (m *Migrator) getNextBlock() (uint, bool) {
	bl := m.block_order.GetNext()
	if bl != -1 {
		return uint(bl), false
	}

	// Migration complete, now do dirty blocks...

	m.queueDirtyBlocks()

	if len(m.blocks_to_move) == 0 {
		return 0, true
	}

	m.blocks_by_n_lock.Lock()
	i := <-m.blocks_to_move
	delete(m.blocks_by_n, i)
	m.blocks_by_n_lock.Unlock()
	return i, false
}
