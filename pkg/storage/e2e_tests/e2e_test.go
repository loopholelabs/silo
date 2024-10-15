package e2e_tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/user"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestNBDMigratorPipe(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	exposed := expose.NewExposedStorageNBDNL(sourceStorage, 8, 0, sourceStorage.Size(), uint64(blockSize), true)

	err = exposed.Init()
	assert.NoError(t, err)

	defer func() {
		err := exposed.Shutdown()
		assert.NoError(t, err)
	}()

	// Write to the nbd device...
	devfile, err := os.OpenFile(fmt.Sprintf("/dev/%s", exposed.Device()), os.O_RDWR, 0666)
	assert.NoError(t, err)

	data_offset := 10
	data_length := 800

	buffer := make([]byte, data_length)
	_, err = rand.Read(buffer)
	assert.NoError(t, err)
	num, err := devfile.WriteAt(buffer, int64(data_offset))
	assert.NoError(t, err)
	assert.Equal(t, 800, num)
	err = devfile.Sync()
	assert.NoError(t, err)
	err = devfile.Close()
	assert.NoError(t, err)

	// Now migrate the data

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

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	assert.Equal(t, int(sourceStorageMem.Size()), destFrom.GetDataPresent())

	read_buffer := make([]byte, data_length)
	_, err = destStorage.ReadAt(read_buffer, int64(data_offset))
	assert.NoError(t, err)

	// Check the data is equal to what we wrote to the nbd device.
	assert.Equal(t, buffer, read_buffer)
}
