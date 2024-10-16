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

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(num_blocks)
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
	num_blocks := (size + blockSize - 1) / blockSize

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

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(num_blocks)
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
func TestMigratorSimplePipe(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

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

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var destStorage storage.StorageProvider
	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.StorageProvider {
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

	prSource := protocol.NewProtocolRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewProtocolRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, initDev)

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

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(num_blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	assert.Equal(t, int(sourceStorageMem.Size()), destFrom.GetDataPresent())
}

/**
 * Test a simple migration through a pipe. No writer no reader.
 * We will tell it we have dirty data and send it.
 */
func TestMigratorSimplePipeDirtySent(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

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

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var destStorage storage.StorageProvider
	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.StorageProvider {
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
			_ = destFrom.HandleDirtyList(func(blocks []uint) {})
		}()
	}

	prSource := protocol.NewProtocolRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewProtocolRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, initDev)

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

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(num_blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	blocks := []uint{0, 6, 7}

	// Send some dirty blocks
	err = destination.DirtyList(conf.Block_size, blocks)
	assert.NoError(t, err)

	err = mig.MigrateDirty(blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	assert.Equal(t, int(sourceStorageMem.Size()), destFrom.GetDataPresent())
}

/**
 * Test a simple migration through a pipe. No writer no reader.
 * We will tell it we have dirty data and send it.
 */
func TestMigratorSimplePipeDirtyMissing(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

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

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var destStorage storage.StorageProvider
	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	var dest_wg sync.WaitGroup

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.StorageProvider {
			destStorage = sources.NewMemoryStorage(int(di.Size))
			return destStorage
		}

		// Pipe from the protocol to destWaiting
		destFrom = protocol.NewFromProtocol(ctx, dev, destStorageFactory, p)
		dest_wg.Add(4)
		go func() {
			_ = destFrom.HandleReadAt()
			dest_wg.Done()
		}()
		go func() {
			_ = destFrom.HandleWriteAt()
			dest_wg.Done()
		}()
		go func() {
			_ = destFrom.HandleDevInfo()
			dest_wg.Done()
		}()
		go func() {
			_ = destFrom.HandleDirtyList(func(blocks []uint) {
			})
			dest_wg.Done()
		}()
	}

	ctx, cancelfn := context.WithCancel(context.TODO())

	prSource := protocol.NewProtocolRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewProtocolRW(ctx, []io.Reader{r2}, []io.Writer{w1}, initDev)

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

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(num_blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	blocks := []uint{0, 6, 7}

	// Send some dirty blocks
	err = destination.DirtyList(conf.Block_size, blocks)
	assert.NoError(t, err)

	// Wait for stuff
	cancelfn()
	dest_wg.Wait()

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	// There should be blocks missing, which equate to len(blocks)*conf.Block_size bytes.

	assert.Equal(t, int(sourceStorageMem.Size())-len(blocks)*conf.Block_size, destFrom.GetDataPresent())
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
	orderer := blocks.NewPriorityBlockOrder(num_blocks, sourceMonitor)

	orderer.AddAll()
	time.Sleep(2000 * time.Millisecond)

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)
	destWaitingLocal, destWaitingRemote := waitingcache.NewWaitingCache(destStorage, blockSize)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock

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

	err = mig.Migrate(num_blocks)
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
	num_blocks := (size + blockSize - 1) / blockSize

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
	destWaitingLocal, destWaitingRemote := waitingcache.NewWaitingCache(destStorage, blockSize)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock
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

	num_local_blocks := 2

	// Write some stuff to local, which will be removed from the list of blocks to migrate
	b := make([]byte, 8192)
	o := 8
	n, err = destWaitingLocal.WriteAt(b, int64(o*blockSize))
	assert.NoError(t, err)
	assert.Equal(t, len(b), n)

	err = mig.Migrate(num_blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	assert.Equal(t, int64(num_blocks-num_local_blocks), mig.metric_blocks_migrated)
}

func TestMigratorSimpleCowSparse(t *testing.T) {
	size := 1024*1024 + 78
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	overlay, err := sources.NewFileStorageSparseCreate("test_migrate_cow", uint64(size), blockSize)
	assert.NoError(t, err)
	cow := modules.NewCopyOnWrite(sourceStorageMem, overlay, blockSize)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(cow, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	t.Cleanup(func() {
		os.Remove("test_migrate_cow")
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

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock
	conf.Error_handler = func(block *storage.BlockInfo, err error) {
		panic(err)
	}

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(num_blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorage, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}
