package testing

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestMigrator(t *testing.T) {
	size := 1024 * 1024
	sourceStorageMem := sources.NewMemoryStorage(size)

	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceMetrics := modules.NewMetrics(sourceStorageMem)
	sourceDirty := modules.NewFilterReadDirtyTracker(sourceMetrics, blockSize)
	sourceMonitor := modules.NewVolatilityMonitor(sourceDirty, blockSize, 100*time.Millisecond)
	sourceStorage := modules.NewLockable(sourceMonitor)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	ctime := time.Now()

	// Periodically write to sourceStorage
	go func() {
		//		ticker := time.NewTicker(20 * time.Millisecond)
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
			/*
				// Perform a write somewhere...
				o := rand.Intn(size * 2)
				// Simulate some non-uniform distribution
				if o > quarter && o < mid {
					o = o - quarter
				}
				if o > size {
					o = o % quarter
				}
			*/
			v := rand.Intn(256)
			b := make([]byte, 1)
			b[0] = byte(v)
			n, err := sourceStorage.WriteAt(b, int64(o))
			assert.NoError(t, err)
			assert.Equal(t, 1, n)
			block_no := o / blockSize
			fmt.Printf(" CONSUMER %dms source.WriteAt(%d) [%d]\n", time.Since(ctime).Milliseconds(), block_no, o)

			fmt.Printf("DATA %d, ,%d\n", time.Now().UnixMilli(), block_no)

			w := rand.Intn(100)

			time.Sleep(time.Duration(w) * time.Millisecond)
		}
	}()

	// Start monitoring blocks, and wait a bit...
	for i := 0; i < num_blocks; i++ {
		sourceMonitor.BlockAvailable(i)
	}
	time.Sleep(5000 * time.Millisecond)

	// START moving data from sourceStorage to destStorage

	locker := func() {
		// This could be used to pause VM/consumer etc...
		sourceStorage.Lock()
	}
	unlocker := func() {
		// Restart consumer
		sourceStorage.Unlock()
	}
	mig := storage.NewMigrator(sourceDirty, blockSize, locker, unlocker)

	metrics := make([]*modules.Metrics, 0)

	//destStorage := sources.NewMemoryStorage(size)
	cr := func(s int) storage.StorageProvider {
		ms := sources.NewMemoryStorage(s)
		msLat := modules.NewArtificialLatency(ms, 0, 10*time.Millisecond)
		msm := modules.NewMetrics(msLat)
		metrics = append(metrics, msm)
		return msm
	}
	destStorage := modules.NewShardedStorage(size, size/32, cr)
	destStorageMetrics := modules.NewMetrics(destStorage)

	// TODO: Currently this is done in a single go with no feedback.
	m_start := time.Now()
	mig.MigrateTo(destStorageMetrics, sourceMonitor.GetNextBlock)
	fmt.Printf("Migration took %d ms\n", time.Since(m_start).Milliseconds())

	// This will end with migration completed, and consumer Locked.
	destStorageMetrics.ShowStats("dest")
	for i, m := range metrics {
		m.ShowStats(fmt.Sprintf(" SHARD%d", i))
	}

	fmt.Printf("Writes STOPPED\n")
	fmt.Printf("Check data is equal\n")

	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

}