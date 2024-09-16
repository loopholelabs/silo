package migrator

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand"
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
	PORT_9000 := testutils.SetupMinio(t.Cleanup)

	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

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
	ctx, cancel_writes := context.WithCancel(context.TODO())
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
	orderer := blocks.NewPriorityBlockOrder(num_blocks, sourceMonitor)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage
	destStorage, err := sources.NewS3StorageCreate(fmt.Sprintf("localhost:%s", PORT_9000), "silosilo", "silosilo", "silosilo", "file", uint64(size), blockSize)

	assert.NoError(t, err)

	sync_config := &Sync_config{
		Name:               "sync_s3",
		Integrity:          false,
		Cancel_writes:      true,
		Dedupe_writes:      true,
		Tracker:            sourceDirtyRemote,
		Lockable:           sourceStorage,
		Destination:        destStorage,
		Orderer:            orderer,
		Dirty_check_period: 1 * time.Second,
		Dirty_block_getter: func() []uint {
			b := sourceDirtyRemote.Sync()
			return b.Collect(0, b.Length())

			// Use some basic params here for getting dirty blocks
			// return sourceDirtyRemote.GetDirtyBlocks(1*time.Second, 16, 10, 4)
		},
		Block_size: blockSize,
		Progress_handler: func(p *MigrationProgress) {
			// Don't need to do anything here...
		},
		Error_handler: func(b *storage.BlockInfo, err error) {
			assert.Fail(t, fmt.Sprintf("Error migrating block %d: %v", b.Block, err))
		},
	}

	// Stop writing in a bit, and we should catch up sync
	time.AfterFunc(time.Second, cancel_writes)

	// Sync the data for a bit
	_, err = Sync(context.TODO(), sync_config, true, false)
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
	num_blocks := (size + blockSize - 1) / blockSize

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

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	// Use sync
	sync_config := &Sync_config{
		Name:               "simple",
		Integrity:          false,
		Cancel_writes:      true,
		Dedupe_writes:      true,
		Tracker:            sourceDirtyRemote,
		Lockable:           sourceStorage,
		Destination:        destStorage,
		Orderer:            orderer,
		Dirty_check_period: 1 * time.Second,
		Dirty_block_getter: func() []uint {
			b := sourceDirtyRemote.Sync()
			return b.Collect(0, b.Length())

			// Use some basic params here for getting dirty blocks
			// return sourceDirtyRemote.GetDirtyBlocks(1*time.Second, 16, 10, 4)
		},
		Block_size: blockSize,
		Progress_handler: func(p *MigrationProgress) {
			// Don't need to do anything here...
		},
		Error_handler: func(b *storage.BlockInfo, err error) {
			assert.Fail(t, fmt.Sprintf("Error migrating block %d: %v", b.Block, err))
		},
	}

	// Sync the data for a bit
	_, err = Sync(context.TODO(), sync_config, true, false)
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}
