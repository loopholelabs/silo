package storage

import (
	"errors"
	"fmt"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/util"
)

type Migrator struct {
	src_track     TrackingStorageProvider
	src_lock_fn   func()
	src_unlock_fn func()
}

func NewMigrator(source TrackingStorageProvider, lock_fn func(), unlock_fn func()) *Migrator {
	return &Migrator{
		src_track:     source,
		src_lock_fn:   lock_fn,
		src_unlock_fn: unlock_fn,
	}
}

/**
 * Migrate storage to dest.
 * This naively continues until all data is synced.
 * It relies on being able to transfer a block quickly before the source changes something again.
 */
func (m *Migrator) MigrateTo(dest StorageProvider, block_size int, block_order func() int) error {
	if dest.Size() != m.src_track.Size() {
		return errors.New("Sizes must be equal for migration")
	}
	num_blocks := (int(m.src_track.Size()) + block_size - 1) / block_size

	blocks_to_move := make(chan uint, num_blocks)
	blocks_by_n := make(map[uint]bool)

	buff := make([]byte, block_size)
	completed_blocks := util.NewBitfield(num_blocks)
	moved_blocks := 0

	ctime := time.Now()

	for {
		// Find out which block we should move next...
		bl := block_order()
		if bl != -1 {
			blocks_to_move <- uint(bl)
			blocks_by_n[uint(bl)] = true
		}

		// No more blocks to be moved! Lets see if we can find any more dirty blocks
		if len(blocks_to_move) == 0 {
			m.src_lock_fn()

			// Check for any dirty blocks to be added on
			blocks := m.src_track.Sync()
			changed := blocks.Count(0, blocks.Length())
			if changed != 0 {
				m.src_unlock_fn()
			}

			fmt.Printf("Got %d more dirty blocks...\n", changed)

			blocks.Exec(0, blocks.Length(), func(pos uint) bool {
				_, ok := blocks_by_n[pos] // Dedup pending by block
				if !ok {
					blocks_to_move <- pos
					blocks_by_n[pos] = true
					completed_blocks.ClearBit(int(pos))
				}
				return true
			})

			// No new dirty blocks, that means we are completely synced, and source is LOCKED
			if changed == 0 {
				break
			}
		}

		completed := completed_blocks.Count(0, uint(num_blocks))
		perc := float64(completed*100) / float64(num_blocks)
		fmt.Printf("Migration %dms %d blocks / %d [%.2f%%]\n", time.Since(ctime).Milliseconds(), completed, num_blocks, perc)

		// TODO: Notify the caller here etc

		i := <-blocks_to_move

		offset := int(i) * block_size
		// Read from source
		n, err := m.src_track.ReadAt(buff, int64(offset))
		if n != block_size || err != nil {
			return err
		}

		// Now write it to destStorage
		n, err = dest.WriteAt(buff, int64(offset))
		if n != block_size || err != nil {
			return err
		}
		// Mark it as done
		completed_blocks.SetBit(int(i))
		delete(blocks_by_n, i)
		moved_blocks++
		fmt.Printf("Block moved %d\n", i)
		fmt.Printf("DATA %d,%d,\n", time.Now().UnixMilli(), i)
	}

	completed := completed_blocks.Count(0, uint(num_blocks))
	perc := float64(completed*100) / float64(num_blocks)
	fmt.Printf("Migration %dms %d blocks / %d [%.2f%%] COMPLETE\n", time.Since(ctime).Milliseconds(), completed, num_blocks, perc)
	fmt.Printf("%d total blocks moved\n", moved_blocks)

	return nil
}
