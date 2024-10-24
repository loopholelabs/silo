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

/**
 * Test a simple migration through a pipe. No writer no reader.
 *
 */
func TestMigratorS3Assisted(t *testing.T) {
	PORT_9000 := testutils.SetupMinio(t.Cleanup)

	size := 1024 * 1024
	blockSize := 64 * 1024

	testSyncSchemaSrc := fmt.Sprintf(`
	device TestSync {
		system = "file"
		size = "%d"
		blocksize = "%d"
		location = "%s"
		sync {
			secure = false
			accesskey = "silosilo"
			secretkey = "silosilo"
			endpoint = "%s"
			bucket = "silosilo"
			config {
				blockshift = 2
				maxage = "100ms"
				minchanged = 4
				limit = 8
				checkperiod = "500ms"
			}
		}
	}
	`, size, blockSize, "./testfile_sync_src", fmt.Sprintf("localhost:%s", PORT_9000))

	testSyncSchemaDest := fmt.Sprintf(`
	device TestSync {
		system = "file"
		size = "%d"
		blocksize = "%d"
		location = "%s"
		sync {
			secure = false
			accesskey = "silosilo"
			secretkey = "silosilo"
			endpoint = "%s"
			bucket = "silosilo"
			config {
				blockshift = 2
				maxage = "100ms"
				minchanged = 4
				limit = 8
				checkperiod = "500ms"
			}
		}
	}
	`, size, blockSize, "./testfile_sync_dest", fmt.Sprintf("localhost:%s", PORT_9000))

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

	prov := devSrc["TestSync"].Provider

	//
	ok := storage.SendSiloEvent(prov, "sync.start", nil)
	assert.Equal(t, 1, len(ok))
	assert.True(t, ok[0].(bool))

	num_blocks := (size + blockSize - 1) / blockSize
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(prov, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	_, err = crand.Read(buffer)
	assert.NoError(t, err)

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Wait for the sync to do some bits.
	time.Sleep(500 * time.Millisecond)

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.StorageProvider {
			return devDest["TestSync"].Provider
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
			_ = destFrom.HandleWriteAtHash()
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

	conf := migrator.NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock

	mig, err := migrator.NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(num_blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will GRAB the data in alternateSources from S3, and return when it's done.
	storage.SendSiloEvent(devDest["TestSync"].Provider, "sync.start", nil)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(devSrc["TestSync"].Provider, devDest["TestSync"].Provider, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	assert.Equal(t, int(prov.Size()), destFrom.GetDataPresent())

	// Get some statistics from the source
	srcStats := storage.SendSiloEvent(devSrc["TestSync"].Provider, "sync.status", nil)
	require.Equal(t, 1, len(srcStats))
	srcMetrics := srcStats[0].(*sources.S3Metrics)

	// Get some statistics from the destination puller
	destStats := storage.SendSiloEvent(devDest["TestSync"].Provider, "sync.status", nil)
	require.Equal(t, 1, len(destStats))
	destMetrics := destStats[0].(*sources.S3Metrics)

	// The source should have pushed some blocks to S3 but not all.
	assert.Greater(t, int(srcMetrics.BlocksWCount), 0)
	assert.Less(t, int(srcMetrics.BlocksWCount), num_blocks)

	// Do some asserts on the S3Metrics... It should have pulled some from S3, but not all
	assert.Greater(t, int(destMetrics.BlocksRCount), 0)
	assert.Less(t, int(destMetrics.BlocksRCount), num_blocks)

}
