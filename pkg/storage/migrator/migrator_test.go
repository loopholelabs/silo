package migrator

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

/**
 * Test a simple migration. No writer no reader.
 *
 */
func TestMigratorSimple(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirty := modules.NewFilterReadDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirty)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)

	for i := 0; i < num_blocks; i++ {
		orderer.Add(i)
	}

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirty,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	mig.Migrate(num_blocks)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)
	mig.ShowProgress()

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}

/**
 * Test a simple migration through a pipe. No writer no reader.
 *
 */
func TestMigratorSimplePipe(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirty := modules.NewFilterReadDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirty)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)

	for i := 0; i < num_blocks; i++ {
		orderer.Add(i)
	}

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	prSource := protocol.NewProtocolRW(context.TODO(), r1, w2)
	prDest := protocol.NewProtocolRW(context.TODO(), r2, w1)

	go prSource.Handle()
	go prDest.Handle()

	// Pipe a destination to the protocol
	destination := modules.NewToProtocol(sourceDirty.Size(), 17, prSource)

	// Pipe from the protocol to destWaiting
	destFrom := modules.NewFromProtocol(17, destStorage, prDest)
	ctx := context.TODO()
	go destFrom.HandleSend(ctx)
	go destFrom.HandleReadAt()
	go destFrom.HandleWriteAt()

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirty,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	mig.Migrate(num_blocks)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)
	mig.ShowProgress()

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}

/**
 * Test a migration with reader and writer.
 *
 */
func TestMigratorWithReaderWriter(t *testing.T) {
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
	destWaitingLocal, destWaitingRemote := modules.NewWaitingCache(destStorage, blockSize)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirty,
		destWaitingRemote,
		orderer,
		conf)

	assert.NoError(t, err)

	// Setup destWaiting to ask for prioritization of blocks as reads come through
	destWaitingLocal.NeedAt = func(offset int64, length int32) {
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

			n, err := destWaitingLocal.ReadAt(b, int64(o))
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

func TestMigratorWithReaderWriterWrite(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirty := modules.NewFilterReadDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirty)

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
	orderer := blocks.NewPriorityBlockOrder(num_blocks, nil)

	for i := 0; i < num_blocks; i++ {
		orderer.Add(i)
		orderer.PrioritiseBlock(i)
	}
	time.Sleep(2000 * time.Millisecond)

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)
	destWaitingLocal, destWaitingRemote := modules.NewWaitingCache(destStorage, blockSize)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock
	// Get rid of concurrency
	conf.Concurrency = map[int]int{storage.BlockTypeAny: 1}

	mig, err := NewMigrator(sourceDirty,
		destWaitingRemote,
		orderer,
		conf)

	assert.NoError(t, err)

	// Setup destWaiting to ask for prioritization of blocks as reads come through
	destWaitingLocal.NeedAt = func(offset int64, length int32) {
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

	destWaitingLocal.DontNeedAt = func(offset int64, length int32) {
		end := uint64(offset + int64(length))
		if end > uint64(size) {
			end = uint64(size)
		}

		b_start := int(offset / int64(blockSize))
		b_end := int((end-1)/uint64(blockSize)) + 1
		for b := b_start; b < b_end; b++ {
			orderer.Remove(b)
		}
	}

	// Set something up to read dest...
	go func() {
		for {
			o := rand.Intn(size)
			b := make([]byte, 1)

			n, err := destWaitingLocal.ReadAt(b, int64(o))
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

	num_local_blocks := 2

	// Write some stuff to local, which will be removed from the list of blocks to migrate
	b := make([]byte, 8192)
	o := 8
	n, err = destWaitingLocal.WriteAt(b, int64(o*blockSize))
	assert.NoError(t, err)
	assert.Equal(t, len(b), n)

	mig.Migrate(num_blocks)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)
	mig.ShowProgress()

	assert.Equal(t, int64(num_blocks-num_local_blocks), mig.metric_moved_blocks)

}
