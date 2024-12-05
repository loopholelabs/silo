package migrator

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/loopholelabs/silo/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestSyncToS3(t *testing.T) {
	MinioPort := testutils.SetupMinio(t.Cleanup)

	size := 1024 * 1024
	blockSize := 4096
	numBlocks := (size + blockSize - 1) / blockSize

	// First we setup some local storage
	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, blockSize, 2*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	// Set up some data here.
	buffer := make([]byte, size)
	_, err := crand.Read(buffer)
	assert.NoError(t, err)

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Periodically write to sourceStorage so it is dirty
	ctx, cancelWrites := context.WithCancel(context.TODO())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			o := rand.Intn(size)
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
	orderer := blocks.NewPriorityBlockOrder(numBlocks, sourceMonitor)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage
	destStorage, err := sources.NewS3StorageCreate(false, fmt.Sprintf("localhost:%s", MinioPort), "silosilo", "silosilo", "silosilo", "file", uint64(size), blockSize)

	assert.NoError(t, err)

	syncConfig := &SyncConfig{
		Name:             "sync_s3",
		Integrity:        false,
		CancelWrites:     true,
		DedupeWrites:     true,
		Tracker:          sourceDirtyRemote,
		Lockable:         sourceStorage,
		Destination:      destStorage,
		Orderer:          orderer,
		DirtyCheckPeriod: 1 * time.Second,
		DirtyBlockGetter: func() []uint {
			b := sourceDirtyRemote.Sync()
			return b.Collect(0, b.Length())

			// Use some basic params here for getting dirty blocks
			// return sourceDirtyRemote.GetDirtyBlocks(1*time.Second, 16, 10, 4)
		},
		BlockSize: blockSize,
		ProgressHandler: func(_ *MigrationProgress) {
			// Don't need to do anything here...
		},
		ErrorHandler: func(b *storage.BlockInfo, err error) {
			assert.Fail(t, fmt.Sprintf("Error migrating block %d: %v", b.Block, err))
		},
	}

	// Stop writing in a bit, and we should catch up sync
	time.AfterFunc(time.Second, cancelWrites)

	// Sync the data for a bit
	syncer := NewSyncer(context.TODO(), syncConfig)
	_, err = syncer.Sync(true, false)
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	assert.True(t, sourceStorage.IsLocked())
}

func TestSyncSimple(t *testing.T) {
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

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	// Use sync
	syncConfig := &SyncConfig{
		Name:             "simple",
		Integrity:        false,
		CancelWrites:     true,
		DedupeWrites:     true,
		Tracker:          sourceDirtyRemote,
		Lockable:         sourceStorage,
		Destination:      destStorage,
		Orderer:          orderer,
		DirtyCheckPeriod: 1 * time.Second,
		DirtyBlockGetter: func() []uint {
			b := sourceDirtyRemote.Sync()
			return b.Collect(0, b.Length())

			// Use some basic params here for getting dirty blocks
			// return sourceDirtyRemote.GetDirtyBlocks(1*time.Second, 16, 10, 4)
		},
		BlockSize: blockSize,
		ProgressHandler: func(_ *MigrationProgress) {
			// Don't need to do anything here...
		},
		ErrorHandler: func(b *storage.BlockInfo, err error) {
			assert.Fail(t, fmt.Sprintf("Error migrating block %d: %v", b.Block, err))
		},
	}

	// Sync the data for a bit
	syncer := NewSyncer(context.TODO(), syncConfig)
	_, err = syncer.Sync(true, false)
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}

func TestSyncSimpleCancel(t *testing.T) {
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

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	// Error out here... APART FROM BLOCK 0 and block 8
	destStorage := sources.NewMemoryStorage(size)
	destHooks := modules.NewHooks(destStorage)
	destHooks.PreWrite = func(_ []byte, offset int64) (bool, int, error) {
		if offset == 0 || offset == (8*int64(blockSize)) {
			return false, 0, nil
		}
		return true, 0, context.Canceled
	}

	// Use sync
	syncConfig := &SyncConfig{
		Concurrency:      map[int]int{storage.BlockTypeAny: 100},
		Name:             "simple",
		Integrity:        false,
		CancelWrites:     true,
		DedupeWrites:     true,
		Tracker:          sourceDirtyRemote,
		Lockable:         sourceStorage,
		Destination:      destHooks,
		Orderer:          orderer,
		DirtyCheckPeriod: 1 * time.Second,
		DirtyBlockGetter: func() []uint {
			b := sourceDirtyRemote.Sync()
			return b.Collect(0, b.Length())
		},
		BlockSize:         blockSize,
		ProgressRateLimit: 0,
		ProgressHandler:   func(_ *MigrationProgress) {},
		ErrorHandler:      func(_ *storage.BlockInfo, _ error) {},
	}

	syncer := NewSyncer(context.TODO(), syncConfig)
	_, err = syncer.Sync(true, false)
	assert.NoError(t, err)

	// Here we should check that any blocks in progress aren't included - only the ones that were successful writes
	bstatus := syncer.GetSafeBlockMap()
	assert.Equal(t, 2, len(bstatus))

	keys := make([]uint, 0)
	for k := range bstatus {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	assert.Equal(t, []uint{0, 8}, keys)
}
