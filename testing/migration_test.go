package testing

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestMigration(t *testing.T) {
	size := 1024 * 1024
	sourceStorageMem := sources.NewMemoryStorage(size)
	destStorage := sources.NewMemoryStorage(size)

	blockSize := 4096

	sourceDirty := modules.NewFilterReadDirtyTracker(sourceStorageMem, blockSize)
	sourceMetrics := modules.NewMetrics(sourceDirty)
	sourceStorage := modules.NewLockable(sourceMetrics)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Periodically write to sourceStorage
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				// Perform a write somewhere...
				o := rand.Intn(size)
				v := rand.Intn(256)
				b := make([]byte, 1)
				b[0] = byte(v)
				n, err := sourceStorage.WriteAt(b, int64(o))
				assert.NoError(t, err)
				assert.Equal(t, 1, n)
				block_no := o / blockSize
				fmt.Printf("source.WriteAt(%d) [%d]\n", block_no, o)
			}
		}
	}()

	// START moving data from sourceStorage to destStorage

	mover := func(block uint) {
		offset := int(block) * blockSize
		buff := make([]byte, blockSize)
		n, err := sourceStorage.ReadAt(buff, int64(offset))
		assert.NoError(t, err)
		assert.Equal(t, blockSize, n)
		// Now write it to destStorage
		n, err = destStorage.WriteAt(buff, int64(offset))
		assert.NoError(t, err)
		assert.Equal(t, blockSize, n)
		fmt.Printf("Moved block from source to dest %d\n", block)
		time.Sleep(5 * time.Millisecond)
	}

	blocks_to_move := make(chan uint, size/blockSize)
	blocks_by_n := make(map[uint]bool)

	// Queue up all blocks to be moved...
	for i := 0; i < size/blockSize; i++ {
		blocks_to_move <- uint(i)
		blocks_by_n[uint(i)] = true
	}

	for {

		// No more blocks to be moved! Lets see if we can find any more dirty blocks
		if len(blocks_to_move) == 0 {
			sourceStorage.Lock()

			// Check for any dirty blocks to be added on
			blocks := sourceDirty.Sync()
			changed := blocks.Count(0, blocks.Length())
			if changed != 0 {
				sourceStorage.Unlock()
			}

			fmt.Printf("Got %d more dirty blocks...\n", changed)

			blocks.Exec(0, blocks.Length(), func(pos uint) bool {
				_, ok := blocks_by_n[pos] // Dedup pending by block
				if !ok {
					blocks_to_move <- pos
					blocks_by_n[pos] = true
				}
				return true
			})

			// No new dirty blocks, that means we are completely synced, and source is LOCKED
			if changed == 0 {
				break
			}
		}

		// Move a block
		i := <-blocks_to_move
		mover(i)
		delete(blocks_by_n, i)
	}

	fmt.Printf("Writes STOPPED\n")
	fmt.Printf("Check data is equal\n")

	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

}
