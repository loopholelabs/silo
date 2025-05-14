package migrator

import (
	"context"
	crand "crypto/rand"
	"io"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/**
 * Test a simple migration. No writer no reader.
 *
 */
func TestMigratorSimple(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	numBlocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}

/**
 * Test a simple migration, but with a partial block at the end
 *
 */
func TestMigratorPartial(t *testing.T) {
	size := 5000
	blockSize := 4096
	numBlocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}

/**
 * Test a simple migration through a pipe. No writer no reader.
 *
 */
func TestMigratorSimplePipe(tt *testing.T) {
	tests := map[string]*protocol.BufferedWriterConfig{
		"noBuffering": nil,
		"someBuffering": {
			Timeout:    50 * time.Millisecond,
			DelayTimer: false,
			MaxLength:  1024 * 1024,
		},
		"delayBuffering": {
			Timeout:    50 * time.Millisecond,
			DelayTimer: true,
			MaxLength:  1024 * 1024,
		},
	}
	for name, cc := range tests {
		tt.Run(name, func(t *testing.T) {
			size := 1024 * 1024
			blockSize := 4096
			numBlocks := (size + blockSize - 1) / blockSize

			sourceStorageMem := sources.NewMemoryStorage(size)
			sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
			sourceStorage := modules.NewLockable(sourceDirtyLocal)

			// Set up some data here.
			buffer := make([]byte, size)
			for i := 0; i < size; i++ {
				buffer[i] = 9
			}

			n, err := sourceStorage.WriteAt(buffer, 0)
			assert.NoError(t, err)
			assert.Equal(t, len(buffer), n)

			orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
			orderer.AddAll()

			// START moving data from sourceStorage to destStorage

			var destStorage storage.Provider
			var destFrom *protocol.FromProtocol

			// Create a simple pipe
			r1, w1 := io.Pipe()
			r2, w2 := io.Pipe()

			initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
				destStorageFactory := func(di *packets.DevInfo) storage.Provider {
					destStorage = sources.NewMemoryStorage(int(di.Size))
					return destStorage
				}

				// Pipe from the protocol to destWaiting
				destFrom = protocol.NewFromProtocol(ctx, dev, destStorageFactory, p)
				go func() {
					_ = destFrom.HandleReadAt()
				}()
				go func() {
					_ = destFrom.HandleWriteAt()
				}()
				go func() {
					_ = destFrom.HandleDevInfo()
				}()
			}

			prSource := protocol.NewRWWithBuffering(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, cc, nil)
			prDest := protocol.NewRWWithBuffering(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, cc, initDev)

			go func() {
				_ = prSource.Handle()
			}()
			go func() {
				_ = prDest.Handle()
			}()

			// Pipe a destination to the protocol
			destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

			err = destination.SendDevInfo("test", uint32(blockSize), "")
			assert.NoError(t, err)

			conf := NewConfig().WithBlockSize(blockSize)
			conf.LockerHandler = sourceStorage.Lock
			conf.UnlockerHandler = sourceStorage.Unlock

			mig, err := NewMigrator(sourceDirtyRemote,
				destination,
				orderer,
				conf)

			assert.NoError(t, err)

			err = mig.Migrate(numBlocks)
			assert.NoError(t, err)

			err = mig.WaitForCompletion()
			assert.NoError(t, err)

			// This will end with migration completed, and consumer Locked.
			eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
			assert.NoError(t, err)
			assert.True(t, eq)

		})
	}
}

/**
 * Test a simple migration through a pipe. No writer no reader.
 * We will tell it we have dirty data and send it.
 */
func TestMigratorSimplePipeDirtySent(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	numBlocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var destStorage storage.Provider
	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.Provider {
			destStorage = sources.NewMemoryStorage(int(di.Size))
			return destStorage
		}

		// Pipe from the protocol to destWaiting
		destFrom = protocol.NewFromProtocol(ctx, dev, destStorageFactory, p)
		go func() {
			_ = destFrom.HandleReadAt()
		}()
		go func() {
			_ = destFrom.HandleWriteAt()
		}()
		go func() {
			_ = destFrom.HandleDevInfo()
		}()
		go func() {
			_ = destFrom.HandleDirtyList(func(_ []uint) {})
		}()
	}

	prSource := protocol.NewRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, initDev)

	go func() {
		_ = prSource.Handle()
	}()
	go func() {
		_ = prDest.Handle()
	}()

	// Pipe a destination to the protocol
	destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

	err = destination.SendDevInfo("test", uint32(blockSize), "")
	assert.NoError(t, err)

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	blocks := []uint{0, 6, 7}

	// Send some dirty blocks
	err = destination.DirtyList(conf.BlockSize, blocks)
	assert.NoError(t, err)

	err = mig.MigrateDirty(blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

}

/**
 * Test a simple migration through a pipe. No writer no reader.
 * We will tell it we have dirty data and send it.
 */
func TestMigratorSimplePipeDirtyMissing(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	numBlocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var destStorage storage.Provider
	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	var destWG sync.WaitGroup

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.Provider {
			destStorage = sources.NewMemoryStorage(int(di.Size))
			return destStorage
		}

		// Pipe from the protocol to destWaiting
		destFrom = protocol.NewFromProtocol(ctx, dev, destStorageFactory, p)
		destWG.Add(4)
		go func() {
			_ = destFrom.HandleReadAt()
			destWG.Done()
		}()
		go func() {
			_ = destFrom.HandleWriteAt()
			destWG.Done()
		}()
		go func() {
			_ = destFrom.HandleDevInfo()
			destWG.Done()
		}()
		go func() {
			_ = destFrom.HandleDirtyList(func(_ []uint) {
			})
			destWG.Done()
		}()
	}

	ctx, cancelfn := context.WithCancel(context.TODO())

	prSource := protocol.NewRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewRW(ctx, []io.Reader{r2}, []io.Writer{w1}, initDev)

	go func() {
		_ = prSource.Handle()
	}()
	go func() {
		_ = prDest.Handle()
	}()

	// Pipe a destination to the protocol
	destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

	err = destination.SendDevInfo("test", uint32(blockSize), "")
	assert.NoError(t, err)

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	blocks := []uint{0, 6, 7}

	// Send some dirty blocks
	err = destination.DirtyList(conf.BlockSize, blocks)
	assert.NoError(t, err)

	// Wait for stuff
	cancelfn()
	destWG.Wait()

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
	numBlocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, blockSize, 2*time.Second)
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

			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
		}
	}()

	// Start monitoring blocks, and wait a bit...
	orderer := blocks.NewPriorityBlockOrder(numBlocks, sourceMonitor)

	orderer.AddAll()
	time.Sleep(2000 * time.Millisecond)

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)
	destWaitingLocal, destWaitingRemote := waitingcache.NewWaitingCache(destStorage, blockSize)

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
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

		bStart := int(offset / int64(blockSize))
		bEnd := int((end-1)/uint64(blockSize)) + 1
		for b := bStart; b < bEnd; b++ {
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
			sn, serr := sourceDirtyLocal.ReadAt(sb, int64(o))
			assert.NoError(t, serr)
			assert.Equal(t, len(sb), sn)

			// Check the data is the same...
			for i, v := range b {
				assert.Equal(t, v, sb[i])
			}

			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		}
	}()

	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	for {
		blocks := mig.GetLatestDirty()
		if blocks == nil {
			break
		}
		destWaitingLocal.DirtyBlocks(blocks)

		err := mig.MigrateDirty(blocks)
		assert.NoError(t, err)
	}

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	assert.True(t, sourceStorage.IsLocked())

}

func TestMigratorWithReaderWriterWrite(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	numBlocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	_, err := crand.Read(buffer)
	assert.NoError(t, err)

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	writeCtx, writeCancel := context.WithCancel(context.Background())
	defer writeCancel()

	// Periodically write to sourceStorage (Make it non-uniform)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-writeCtx.Done():
				return
			case <-ticker.C:
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
			}
		}
	}()

	// Start monitoring blocks, and wait a bit...
	orderer := blocks.NewPriorityBlockOrder(numBlocks, nil)

	for i := 0; i < numBlocks; i++ {
		orderer.Add(i)
		orderer.PrioritiseBlock(i)
	}
	time.Sleep(2000 * time.Millisecond)

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)
	destWaitingLocal, destWaitingRemote := waitingcache.NewWaitingCache(destStorage, blockSize)

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock
	// Get rid of concurrency
	conf.Concurrency = map[int]int{storage.BlockTypeAny: 1}

	mig, err := NewMigrator(sourceDirtyRemote,
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

		bStart := int(offset / int64(blockSize))
		bEnd := int((end-1)/uint64(blockSize)) + 1
		for b := bStart; b < bEnd; b++ {
			// Ask the orderer to prioritize these blocks...
			orderer.PrioritiseBlock(b)
		}
	}

	destWaitingLocal.DontNeedAt = func(offset int64, length int32) {
		end := uint64(offset + int64(length))
		if end > uint64(size) {
			end = uint64(size)
		}

		bStart := int(offset / int64(blockSize))
		bEnd := int((end-1)/uint64(blockSize)) + 1
		for b := bStart; b < bEnd; b++ {
			orderer.Remove(b)
		}
	}

	// Set something up to read dest...
	readContext, readCancel := context.WithCancel(context.Background())
	defer readCancel()

	go func() {
		for {
			select {
			case <-readContext.Done():
				return
			default:
			}
			o := rand.Intn(size)
			b := make([]byte, 1)

			n, err := destWaitingLocal.ReadAt(b, int64(o))
			assert.NoError(t, err)
			assert.Equal(t, len(b), n)

			// Check it's the same value as from SOURCE...
			sb := make([]byte, len(b))
			sn, serr := sourceDirtyLocal.ReadAt(sb, int64(o))
			assert.NoError(t, serr)
			assert.Equal(t, len(sb), sn)

			// NOTE. The data may not be the same at this point if it's become dirty in the source.

			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		}
	}()

	numLocalBlocks := 2

	// Write some stuff to local, which will be removed from the list of blocks to migrate
	b := make([]byte, 8192)
	_, err = crand.Read(b)
	assert.NoError(t, err)
	o := 8
	n, err = destWaitingLocal.WriteAt(b, int64(o*blockSize))
	assert.NoError(t, err)
	assert.Equal(t, len(b), n)

	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	for {
		blocks := mig.GetLatestDirty()
		if blocks == nil {
			break
		}
		destWaitingLocal.DirtyBlocks(blocks)

		err := mig.MigrateDirty(blocks)
		assert.NoError(t, err)
	}

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will finish with source locked

	require.True(t, sourceStorage.IsLocked())

	assert.Equal(t, numBlocks-numLocalBlocks, mig.migratedBlocks.Count(0, mig.migratedBlocks.Length()))

	// Update the source as well...
	n, err = sourceDirtyLocal.WriteAt(b, int64(o*blockSize))
	assert.NoError(t, err)
	assert.Equal(t, len(b), n)

	// Check the storage is equal?
	eq, err := storage.Equals(destWaitingLocal, sourceDirtyLocal, 4096)
	assert.NoError(t, err)
	assert.True(t, eq)
}

func TestMigratorSimpleCowSparse(t *testing.T) {
	size := 1024*1024 + 78
	blockSize := 4096
	numBlocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	overlay, err := sources.NewFileStorageSparseCreate("test_simple_migrate_cow", uint64(size), blockSize)
	assert.NoError(t, err)
	cow := modules.NewCopyOnWrite(sourceStorageMem, overlay, blockSize, true, nil)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(cow, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	t.Cleanup(func() {
		os.Remove("test_simple_migrate_cow")
	})

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	n, err := sourceStorageMem.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Write some things to the overlay as well...

	buffer = make([]byte, 5000)
	_, err = crand.Read(buffer)
	assert.NoError(t, err)
	n, err = sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	buffer2 := make([]byte, 5000)
	_, err = crand.Read(buffer2)
	assert.NoError(t, err)
	n, err = sourceStorage.WriteAt(buffer2, 100)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	buffer2 = make([]byte, 5000)
	_, err = crand.Read(buffer2)
	assert.NoError(t, err)
	n, err = sourceStorage.WriteAt(buffer2, int64(size)-4000)
	assert.NoError(t, err)
	assert.Equal(t, 4000, n)

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	migrateBlocks := numBlocks
	// Find unrequired blocks, and remove from the migration
	unrequired := sourceDirtyRemote.GetUnrequiredBlocks()
	for _, b := range unrequired {
		orderer.Remove(int(b))
		// Send the data here...
		// We need less blocks here...
		migrateBlocks--
	}

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)
	destCow := modules.NewCopyOnWrite(sourceStorageMem, destStorage, blockSize, true, nil)

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock
	conf.ErrorHandler = func(_ *storage.BlockInfo, err error) {
		panic(err)
	}

	mig, err := NewMigrator(sourceDirtyRemote,
		destCow,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(migrateBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorage, destCow, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}
