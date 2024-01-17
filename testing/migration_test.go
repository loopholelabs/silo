package testing

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

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
				fmt.Printf("source.WriteAt() [%d]\n", o)
			}
		}
	}()

	// START moving data from sourceStorage to destStorage

	buff := make([]byte, blockSize)
	for i := 0; i < size; i += blockSize {
		n, err := sourceStorage.ReadAt(buff, int64(i))
		assert.NoError(t, err)
		assert.Equal(t, blockSize, n)
		// Now write it to destStorage
		n, err = destStorage.WriteAt(buff, int64(i))
		assert.NoError(t, err)
		assert.Equal(t, blockSize, n)
		fmt.Printf("Moved block from source to dest %d / %d\n", i, size)
		time.Sleep(5 * time.Millisecond)
	}

	// Get dirty blocks
	for {
		blocks := sourceDirty.Sync()

		fmt.Printf("Got %d dirty blocks...\n", blocks.Count(0, blocks.Length()))
		changed := 0

		// Transfer any dirty blocks to dest
		for i := 0; i < size; i += blockSize {
			block_no := i / blockSize
			if blocks.BitSet(block_no) {
				changed++
				n, err := sourceMetrics.ReadAt(buff, int64(i)) // Read from outside the lock
				assert.NoError(t, err)
				assert.Equal(t, blockSize, n)
				// Now write it to destStorage
				n, err = destStorage.WriteAt(buff, int64(i))
				assert.NoError(t, err)
				assert.Equal(t, blockSize, n)
				fmt.Printf("Moved dirty block %d %d\n", i, i+blockSize)
				time.Sleep(5 * time.Millisecond)
			}
		}
		if changed == 0 {
			break // Nothing changed, lets go.
		}
	}

	// NOW stop writes to sourceStorage
	sourceStorage.Lock()

	fmt.Printf("Writes STOPPED\n")

	fmt.Printf("Check data is equal\n")

	// Assert the data in source and dest are equal
	sourceBuff := make([]byte, blockSize)
	destBuff := make([]byte, blockSize)
	for i := 0; i < size; i += blockSize {
		n, err := sourceStorageMem.ReadAt(sourceBuff, int64(i))
		assert.NoError(t, err)
		assert.Equal(t, blockSize, n)
		n, err = destStorage.ReadAt(destBuff, int64(i))
		assert.NoError(t, err)
		assert.Equal(t, blockSize, n)
		for j := 0; j < blockSize; j++ {
			assert.Equal(t, sourceBuff[j], destBuff[j])
		}
	}
}
