package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/util"
)

type Migrator struct {
	src_track           TrackingStorageProvider
	src_lock_fn         func()
	src_unlock_fn       func()
	block_size          int
	metric_moved_blocks int
	completed_blocks    *util.Bitfield
}

func NewMigrator(source TrackingStorageProvider, block_size int, lock_fn func(), unlock_fn func()) *Migrator {
	return &Migrator{
		src_track:           source,
		src_lock_fn:         lock_fn,
		src_unlock_fn:       unlock_fn,
		block_size:          block_size,
		metric_moved_blocks: 0,
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
func (m *Migrator) MigrateTo(dest StorageProvider, block_order func() int) error {
	if dest.Size() != m.src_track.Size() {
		return errors.New("Sizes must be equal for migration")
	}
	num_blocks := (int(m.src_track.Size()) + m.block_size - 1) / m.block_size

	blocks_to_move := make(chan uint, num_blocks)
	blocks_by_n := make(map[uint]bool)
	var blocks_by_n_lock sync.Mutex

	m.completed_blocks = util.NewBitfield(num_blocks)
	ctime := time.Now()

	concurrency := make(chan bool, 256) // max concurrency
	var wg sync.WaitGroup

	for {
		// TODO: Check if we have any priority blocks we need to move...

		// Find out which block we should migrate next...
		//		if len(blocks_to_move) == 0 {
		bl := block_order()
		if bl != -1 {
			// NB We don't need to dedup here, because it cannot be dirty yet.
			blocks_by_n_lock.Lock()
			blocks_to_move <- uint(bl)
			blocks_by_n[uint(bl)] = true
			blocks_by_n_lock.Unlock()
		}
		//		}

		// No more blocks to be moved! Lets see if we can find any more dirty blocks
		if len(blocks_to_move) == 0 {
			m.src_lock_fn()

			// Check for any dirty blocks to be added on
			blocks := m.src_track.Sync()
			changed := blocks.Count(0, blocks.Length())
			if changed != 0 {
				m.src_unlock_fn()

				fmt.Printf("Got %d more dirty blocks...\n", changed)

				blocks.Exec(0, blocks.Length(), func(pos uint) bool {
					blocks_by_n_lock.Lock()
					_, ok := blocks_by_n[pos] // Dedup pending by block
					if !ok {
						blocks_to_move <- pos
						blocks_by_n[pos] = true
						m.completed_blocks.ClearBit(int(pos))
					}
					blocks_by_n_lock.Unlock()
					return true
				})
				m.showProgress(ctime, num_blocks)

			} else {
				// No new dirty blocks, that means we are completely synced, and source is LOCKED
				break
			}
		}

		if len(blocks_to_move) == 0 {
			// Nothing left to do! Lets quit.
			break
		}

		concurrency <- true
		wg.Add(1)

		blocks_by_n_lock.Lock()
		i := <-blocks_to_move
		delete(blocks_by_n, i)
		blocks_by_n_lock.Unlock()

		go func(block_no uint) {
			err := m.migrateBlock(dest, int(block_no))
			if err != nil {
				fmt.Printf("ERROR moving block %v\n", err)
			}

			m.showProgress(ctime, num_blocks)

			// TODO: If we have a notify for this block, let everyone know it's available

			fmt.Printf("Block moved %d\n", block_no)
			fmt.Printf("DATA %d,%d,\n", time.Now().UnixMilli(), block_no)
			wg.Done()
			<-concurrency
		}(i)
	}

	fmt.Printf("Waiting for pending transfers...\n")
	wg.Wait()

	m.showProgress(ctime, num_blocks)

	return nil
}

/**
 * Show progress...
 *
 */
func (m *Migrator) showProgress(ctime time.Time, num_blocks int) {
	completed := m.completed_blocks.Count(0, uint(num_blocks))
	perc := float64(completed*100) / float64(num_blocks)
	is_complete := ""
	if completed == num_blocks {
		is_complete = " COMPLETE"
	}
	fmt.Printf("Migration %dms %d blocks / %d [%.2f%%]%s\n", time.Since(ctime).Milliseconds(), completed, num_blocks, perc, is_complete)
}

/**
 * Migrate a single block to dest
 *
 */
func (m *Migrator) migrateBlock(dest StorageProvider, block int) error {
	buff := make([]byte, m.block_size)
	offset := int(block) * m.block_size
	// Read from source
	n, err := m.src_track.ReadAt(buff, int64(offset))
	if n != m.block_size || err != nil {
		return err
	}

	// Now write it to destStorage
	n, err = dest.WriteAt(buff, int64(offset))
	if n != m.block_size || err != nil {
		return err
	}

	m.metric_moved_blocks++
	// Mark it as done
	m.completed_blocks.SetBit(block)
	return nil
}
