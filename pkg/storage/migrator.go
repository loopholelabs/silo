package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/util"
)

type MigratorConfig struct {
}

type Migrator struct {
	src_track           TrackingStorageProvider
	dest                StorageProvider
	src_lock_fn         func()
	src_unlock_fn       func()
	block_size          int
	num_blocks          int
	metric_moved_blocks int64
	moving_blocks       *util.Bitfield
	migrated_blocks     *util.Bitfield
	clean_blocks        *util.Bitfield
	block_order         BlockOrder
	ctime               time.Time

	// Our queue for dirty blocks (remigration)
	blocks_to_move chan uint
	blocks_by_n    map[uint]bool

	concurrency map[int]chan bool
	wg          sync.WaitGroup
}

func NewMigrator(source TrackingStorageProvider,
	dest StorageProvider,
	block_size int,
	lock_fn func(),
	unlock_fn func(),
	block_order BlockOrder) *Migrator {

	// TODO: Pass in configuration...
	concurrency_by_block := map[int]int{
		BlockTypeAny:      32,
		BlockTypeStandard: 32,
		BlockTypeDirty:    100,
		BlockTypePriority: 16,
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
		blocks_to_move:      make(chan uint, num_blocks),
		blocks_by_n:         make(map[uint]bool),
		concurrency:         make(map[int]chan bool),
	}

	if m.dest.Size() != m.src_track.Size() {
		// TODO: Return as error
		panic("Sizes must be equal for migration")
	}

	// Initialize concurrency channels
	for b, v := range concurrency_by_block {
		m.concurrency[b] = make(chan bool, v)
	}
	return m
}

/**
 * Migrate storage to dest.
 */
func (m *Migrator) Migrate() error {
	m.ctime = time.Now()

	for {
		i := m.block_order.GetNext()
		if i == BlockInfoFinish {
			break
		}

		cc, ok := m.concurrency[i.Type]
		if !ok {
			cc = m.concurrency[BlockTypeAny]
		}
		cc <- true

		m.wg.Add(1)

		go func(block_no *BlockInfo) {
			err := m.migrateBlock(block_no.Block)
			if err != nil {
				// TODO: Collect errors properly. Abort? Retry? fail?
				fmt.Printf("ERROR moving block %v\n", err)
			}

			m.showProgress()

			fmt.Printf("Block moved %d\n", block_no)
			fmt.Printf("DATA %d,%d,,,,\n", time.Now().UnixMilli(), block_no)
			m.wg.Done()

			cc, ok := m.concurrency[block_no.Type]
			if !ok {
				cc = m.concurrency[BlockTypeAny]
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
		i := &BlockInfo{Block: int(pos), Type: BlockTypeDirty}

		cc, ok := m.concurrency[i.Type]
		if !ok {
			cc = m.concurrency[BlockTypeAny]
		}
		cc <- true

		m.wg.Add(1)

		go func(block_no *BlockInfo) {
			err := m.migrateBlock(block_no.Block)
			if err != nil {
				// TODO: Collect errors properly. Abort? Retry? fail?
				fmt.Printf("ERROR moving block %v\n", err)
			}

			m.showProgress()

			fmt.Printf("Dirty block moved %d\n", block_no)
			fmt.Printf("DATA %d,%d,,,,\n", time.Now().UnixMilli(), block_no)
			m.wg.Done()

			cc, ok := m.concurrency[block_no.Type]
			if !ok {
				cc = m.concurrency[BlockTypeAny]
			}
			<-cc
		}(i)

		m.clean_blocks.ClearBit(int(pos))

		m.showProgress()
	}

	return nil
}

func (m *Migrator) WaitForCompletion() error {
	fmt.Printf("Waiting for pending transfers...\n")
	m.wg.Wait()
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
