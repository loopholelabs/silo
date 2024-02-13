package migrator

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestMigrator(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirty := modules.NewFilterReadDirtyTracker(sourceStorageMem, blockSize)
	sourceMonitor := modules.NewVolatilityMonitor(sourceDirty, blockSize, 2*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Periodically write to sourceStorage (Make it non-uniform)
	go func() {
		for {
			mid := size / 2
			quarter := size / 4

			var o int
			area := rand.Intn(4)
			if area == 0 {
				// random in upper half
				o = mid + rand.Intn(mid)
			} else {
				// random in the lower quarter
				v := rand.Float64() * math.Pow(float64(quarter), 8)
				o = quarter + int(math.Sqrt(math.Sqrt(math.Sqrt(v))))
			}

			v := rand.Intn(256)
			b := make([]byte, 1)
			b[0] = byte(v)
			n, err := sourceStorage.WriteAt(b, int64(o))
			assert.NoError(t, err)
			assert.Equal(t, 1, n)

			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}()

	// Start monitoring blocks, and wait a bit...
	orderer := blocks.NewPriorityBlockOrder(num_blocks, sourceMonitor)

	for i := 0; i < num_blocks; i++ {
		orderer.Add(i)
	}
	time.Sleep(2000 * time.Millisecond)

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)
	destWaiting := modules.NewWaitingCache(destStorage, blockSize)

	mig, err := NewMigrator(sourceDirty,
		destWaiting,
		blockSize,
		sourceStorage.Lock,
		sourceStorage.Unlock,
		orderer)

	assert.NoError(t, err)

	// Setup destWaiting to ask for prioritization of blocks as reads come through
	destWaiting.NeedAt = func(offset int64, length int32) {
		end := uint64(offset + int64(length))
		if end > uint64(size) {
			end = uint64(size)
		}

		b_start := int(offset / int64(blockSize))
		b_end := int((end-1)/uint64(blockSize)) + 1
		for b := b_start; b < b_end; b++ {
			// Ask the orderer to prioritize these blocks...
			orderer.PrioritiseBlock(b)
		}
	}

	// Set something up to read dest...
	go func() {
		for {
			o := rand.Intn(size)
			b := make([]byte, 1)

			n, err := destWaiting.ReadAt(b, int64(o))
			assert.NoError(t, err)
			assert.Equal(t, len(b), n)

			// Check it's the same value as from SOURCE...
			sb := make([]byte, len(b))
			sn, serr := sourceDirty.ReadAt(sb, int64(o))
			assert.NoError(t, serr)
			assert.Equal(t, len(sb), sn)

			// Check the data is the same...
			for i, v := range b {
				assert.Equal(t, v, sb[i])
			}

			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		}
	}()

	mig.Migrate(num_blocks)

	for {
		blocks := mig.GetLatestDirty()
		if blocks == nil {
			break
		}
		fmt.Printf("Got %d dirty blocks to move...\n", len(blocks))
		err := mig.MigrateDirty(blocks)
		assert.NoError(t, err)
		mig.ShowProgress()
	}

	err = mig.WaitForCompletion()
	assert.NoError(t, err)
	mig.ShowProgress()

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	assert.True(t, sourceStorage.IsLocked())

}
