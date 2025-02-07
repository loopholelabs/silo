package migrator_test

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDevices(t *testing.T, size int, blockSize int) (storage.Provider, storage.Provider) {
	MinioPort := testutils.SetupMinio(t.Cleanup)

	testSyncSchemaSrc := fmt.Sprintf(`
	device TestSync {
		system = "file"
		size = "%d"
		blocksize = "%d"
		location = "%s"
		sync {
			secure = false
			autostart = true
			accesskey = "silosilo"
			secretkey = "silosilo"
			endpoint = "%s"
			bucket = "silosilo"
			grabconcurrency = 10
			config {
			    onlydirty = true
				blockshift = 2
				maxage = "100ms"
				minchanged = 4
				limit = 8
				checkperiod = "1s"
				concurrency = 10
			}
		}
	}
	`, size, blockSize, "./testfile_sync_src", fmt.Sprintf("localhost:%s", MinioPort))

	testSyncSchemaDest := fmt.Sprintf(`
	device TestSync {
		system = "file"
		size = "%d"
		blocksize = "%d"
		location = "%s"
		sync {
			secure = false
			autostart = false
			accesskey = "silosilo"
			secretkey = "silosilo"
			endpoint = "%s"
			bucket = "silosilo"
			grabconcurrency = 10
			config {
			    onlydirty = true
				blockshift = 2
				maxage = "100ms"
				minchanged = 4
				limit = 8
				checkperiod = "1s"
				concurrency = 10
			}
		}
	}
	`, size, blockSize, "./testfile_sync_dest", fmt.Sprintf("localhost:%s", MinioPort))

	sSrc := new(config.SiloSchema)
	err := sSrc.Decode([]byte(testSyncSchemaSrc))
	assert.NoError(t, err)
	devSrc, err := device.NewDevices(sSrc.Device)
	assert.NoError(t, err)

	sDest := new(config.SiloSchema)
	err = sDest.Decode([]byte(testSyncSchemaDest))
	assert.NoError(t, err)
	devDest, err := device.NewDevices(sDest.Device)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("./testfile_sync_src")
		os.Remove("./testfile_sync_dest")
	})

	require.Equal(t, 1, len(devSrc))
	require.Equal(t, 1, len(devDest))

	// Make sure the sync is running on src, but NOT on dest.
	assert.Equal(t, true, storage.SendSiloEvent(devSrc["TestSync"].Provider, storage.EventSyncRunning, nil)[0].(bool))
	assert.Equal(t, false, storage.SendSiloEvent(devDest["TestSync"].Provider, storage.EventSyncRunning, nil)[0].(bool))

	return devSrc["TestSync"].Provider, devDest["TestSync"].Provider
}

/**
 * Test a simple migration through a pipe. No writer no reader.
 *
 */
func TestMigratorS3Assisted(t *testing.T) {
	size := 1024 * 1024
	blockSize := 64 * 1024

	provSrc, provDest := setupDevices(t, size, blockSize)

	numBlocks := (size + blockSize - 1) / blockSize
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(provSrc, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	_, err := crand.Read(buffer)
	assert.NoError(t, err)

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Wait for the sync to do some bits.
	time.Sleep(2 * time.Second)

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(_ *packets.DevInfo) storage.Provider {
			return provDest
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
			_ = destFrom.HandleEvent(func(_ *packets.Event) {})
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

	conf := migrator.NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := migrator.NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	destination.SendEvent(&packets.Event{Type: packets.EventCompleted})

	// This will GRAB the data in alternateSources from S3, and return when it's done.
	// storage.SendSiloEvent(provDest, "sync.start", device.SyncStartConfig{AlternateSources: destFrom.GetAlternateSources(), Destination: provDest})

	// WAIT for the sync to be running
	for {
		syncRunning := storage.SendSiloEvent(provDest, storage.EventSyncRunning, nil)[0].(bool)
		if syncRunning {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(provSrc, provDest, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	// Get some statistics from the source
	srcStats := storage.SendSiloEvent(provSrc, storage.EventSyncStatus, nil)
	require.Equal(t, 1, len(srcStats))
	srcMetrics := srcStats[0].(*sources.S3Metrics)

	// Get some statistics from the destination puller
	destStats := storage.SendSiloEvent(provDest, storage.EventSyncStatus, nil)
	require.Equal(t, 1, len(destStats))
	destMetrics := destStats[0].(*sources.S3Metrics)

	// The source should have pushed some blocks to S3 but not all.
	assert.Greater(t, int(srcMetrics.BlocksWCount), 0)
	assert.Less(t, int(srcMetrics.BlocksWCount), numBlocks)

	// Do some asserts on the S3Metrics... It should have pulled some from S3, but not all
	assert.Greater(t, int(destMetrics.BlocksRCount), 0)
	assert.Less(t, int(destMetrics.BlocksRCount), numBlocks)

	// Sync should be running on the dest and NOT on src
	assert.Equal(t, false, storage.SendSiloEvent(provSrc, storage.EventSyncRunning, nil)[0].(bool))
	assert.Equal(t, true, storage.SendSiloEvent(provDest, storage.EventSyncRunning, nil)[0].(bool))

	err = provDest.Close()
	assert.NoError(t, err)
	assert.Equal(t, false, storage.SendSiloEvent(provDest, storage.EventSyncRunning, nil)[0].(bool))

}

/**
 * Once we stop the sync, we change the source. It shouldn't use the alternate source for the data in question
 *
 */
func TestMigratorS3AssistedChangeSource(t *testing.T) {
	size := 1024 * 1024
	blockSize := 64 * 1024

	provSrc, provDest := setupDevices(t, size, blockSize)

	numBlocks := (size + blockSize - 1) / blockSize
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(provSrc, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	_, err := crand.Read(buffer)
	assert.NoError(t, err)

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Wait for the sync to do some bits.
	time.Sleep(2 * time.Second)

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(_ *packets.DevInfo) storage.Provider {
			return provDest
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
			_ = destFrom.HandleEvent(func(_ *packets.Event) {})
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

	conf := migrator.NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := migrator.NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(0) // This will *start* the migration, which will stop the sync and snapshot alternateSources
	assert.NoError(t, err)

	// Now do some writes to source - we'll overwrite half the data, which will have to go p2p instead of S3.
	buffer = make([]byte, size/2)
	_, err = crand.Read(buffer)
	assert.NoError(t, err)

	n, err = sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Do the migration here...
	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	destination.SendEvent(&packets.Event{Type: packets.EventCompleted})

	// This will GRAB the data in alternateSources from S3, and return when it's done.
	// storage.SendSiloEvent(provDest, "sync.start", device.SyncStartConfig{AlternateSources: destFrom.GetAlternateSources(), Destination: provDest})

	// WAIT for the sync to be running
	for {
		syncRunning := storage.SendSiloEvent(provDest, storage.EventSyncRunning, nil)[0].(bool)
		if syncRunning {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(provSrc, provDest, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	// Get some statistics from the source
	srcStats := storage.SendSiloEvent(provSrc, storage.EventSyncStatus, nil)
	require.Equal(t, 1, len(srcStats))
	srcMetrics := srcStats[0].(*sources.S3Metrics)

	// Get some statistics from the destination puller
	destStats := storage.SendSiloEvent(provDest, storage.EventSyncStatus, nil)
	require.Equal(t, 1, len(destStats))
	destMetrics := destStats[0].(*sources.S3Metrics)

	// The source should have pushed some blocks to S3 but not all.
	assert.Greater(t, int(srcMetrics.BlocksWCount), 0)
	assert.Less(t, int(srcMetrics.BlocksWCount), numBlocks)

	// Do some asserts on the S3Metrics... It should have pulled some from S3, but not all
	assert.Greater(t, int(destMetrics.BlocksRCount), 0)
	assert.Less(t, int(destMetrics.BlocksRCount), numBlocks)

	// Sync should be running on the dest and NOT on src
	assert.Equal(t, false, storage.SendSiloEvent(provSrc, storage.EventSyncRunning, nil)[0].(bool))
	assert.Equal(t, true, storage.SendSiloEvent(provDest, storage.EventSyncRunning, nil)[0].(bool))

	err = provDest.Close()
	assert.NoError(t, err)
	assert.Equal(t, false, storage.SendSiloEvent(provDest, storage.EventSyncRunning, nil)[0].(bool))

}
