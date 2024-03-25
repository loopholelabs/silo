package migrator

import (
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

func TestMigratorToS3(t *testing.T) {
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
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}
	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Periodically write to sourceStorage so it is dirty
	go func() {
		for {
			o := rand.Intn(size)
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

	// START moving data from sourceStorage to destStorage
	destStorage, err := sources.NewS3StorageCreate(fmt.Sprintf("localhost:%s", PORT_9000), "silosilo", "silosilo", "silosilo", "file", uint64(size), blockSize)

	assert.NoError(t, err)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	mig.Migrate(num_blocks)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// Wait a bit for source to become dirty
	time.Sleep(1000 * time.Millisecond)

	for {
		blocks := mig.GetLatestDirty()
		if blocks == nil {
			break
		}

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
