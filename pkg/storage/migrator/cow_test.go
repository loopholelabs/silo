package migrator

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestMigratorCow(t *testing.T) {
	size := 10 * 1024 * 1024
	blockSize := 4096
	numBlocks := (size + blockSize - 1) / blockSize

	sourceStorageBaseMem := sources.NewMemoryStorage(size)

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	_, err := crand.Read(buffer)
	assert.NoError(t, err)

	n, err := sourceStorageBaseMem.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	n, err = sourceStorageMem.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Change a few bits in sourceStorageMem

	chgdata := make([]byte, 4096)
	_, err = crand.Read(chgdata)
	assert.NoError(t, err)

	for _, off := range []int64{0, 65536, 80913} {
		n, err = sourceStorageMem.WriteAt(chgdata, off)
		assert.NoError(t, err)
		assert.Equal(t, len(chgdata), n)
	}

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

	prSource := protocol.NewRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, initDev)

	go func() {
		_ = prSource.Handle()
	}()
	go func() {
		_ = prDest.Handle()
	}()

	// Pipe a destination to the protocol
	//	destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

	destination := protocol.NewToProtocolWithBase(sourceDirtyRemote.Size(), 17, prSource, sourceStorageBaseMem)

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

	srcDataSent := prSource.GetMetrics().DataSent
	srcDataRecv := prSource.GetMetrics().DataRecv

	fmt.Printf("Transfer [device size %d] transfer bytes: %d sent, %d recv\n", size, srcDataSent, srcDataRecv)

	assert.Less(t, srcDataSent, uint64(size))

	destMetrics := destination.GetMetrics()

	fmt.Printf("Sent %d WriteAt     %d bytes\n", destMetrics.SentWriteAt, destMetrics.SentWriteAtBytes)
	fmt.Printf("Sent %d WriteAtComp %d bytes\n", destMetrics.SentWriteAtComp, destMetrics.SentWriteAtCompBytes)
	fmt.Printf("Sent %d WriteAtHash %d bytes\n", destMetrics.SentWriteAtHash, destMetrics.SentWriteAtHashBytes)
}
