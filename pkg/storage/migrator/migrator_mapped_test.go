package migrator

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"math/rand"
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

/**
 * Test a simple migration of mapped storage. (Mapped storage being a series of blocks which also have IDs associated with them)
 *
 */
func TestMigratorSimpleMapped(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	mappedStorage := modules.NewMappedStorage(sourceStorage, blockSize)

	// Setup some data here...
	for i := 0; i < 10; i++ {
		buffer := make([]byte, blockSize)
		_, err := crand.Read(buffer)
		assert.NoError(t, err)

		id := rand.Int63n(0x100000000)
		err = mappedStorage.WriteBlock(uint64(id), buffer)
		assert.NoError(t, err)
	}

	usedBlocks := (int(mappedStorage.Size()) + blockSize - 1) / blockSize

	orderer := blocks.NewAnyBlockOrder(usedBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)
	destMappedStorage := modules.NewMappedStorage(destStorage, blockSize)

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	writer := func(data []byte, offset int64, idmap map[uint64]uint64) (int, error) {
		fmt.Printf("Write offset %d, idmap %v\n", offset, idmap)

		// Write to the destination map and storage...
		destMappedStorage.AppendMap(idmap)
		return destStorage.WriteAt(data, offset)
		// return len(data), nil
	}
	mig.SetSourceMapped(mappedStorage, writer)

	// Migrate only the blocks we need...
	err = mig.Migrate(usedBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	// Make sure the maps are the same...
	srcMap := mappedStorage.GetMap()
	destMap := destMappedStorage.GetMap()
	assert.Equal(t, srcMap, destMap)

	migMetrics := mig.GetMetrics()
	fmt.Printf("Migration total migrated blocks %d\n", migMetrics.TotalMigratedBlocks)
}

func TestMigratorPipeSimpleMapped(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	mappedStorage := modules.NewMappedStorage(sourceStorage, blockSize)

	// Setup some data here...
	for i := 0; i < 10; i++ {
		buffer := make([]byte, blockSize)
		_, err := crand.Read(buffer)
		assert.NoError(t, err)

		id := rand.Int63n(0x100000000)
		err = mappedStorage.WriteBlock(uint64(id), buffer)
		assert.NoError(t, err)
	}

	usedBlocks := (int(mappedStorage.Size()) + blockSize - 1) / blockSize

	orderer := blocks.NewAnyBlockOrder(usedBlocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage
	// Through a pipe

	var destStorage *sources.MemoryStorage
	var destMappedStorage *modules.MappedStorage
	var destFrom *protocol.FromProtocol

	conf := NewConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.Provider {
			destStorage = sources.NewMemoryStorage(int(di.Size))
			destMappedStorage = modules.NewMappedStorage(destStorage, blockSize)
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
			_ = destFrom.HandleWriteAtWithMap(func(offset int64, data []byte, idmap map[uint64]uint64) error {
				fmt.Printf("Write offset %d, idmap %v\n", offset, idmap)
				// Write to the destination map and storage...
				destMappedStorage.AppendMap(idmap)
				_, err := destStorage.WriteAt(data, offset)
				return err
			})
		}()
		go func() {
			_ = destFrom.HandleRemoveFromMap(func(ids []uint64) {
				for _, id := range ids {
					fmt.Printf("Remove block %d\n", id)
					destMappedStorage.RemoveBlock(id)
				}
			})
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

	destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

	err := destination.SendDevInfo("test", uint32(blockSize), "")
	assert.NoError(t, err)

	mig, err := NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	writer := func(data []byte, offset int64, idmap map[uint64]uint64) (int, error) {
		return destination.WriteAtWithMap(data, offset, idmap)
	}
	mig.SetSourceMapped(mappedStorage, writer)

	// Now set the migration on its way.

	// Migrate only the blocks we need...
	err = mig.Migrate(usedBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// Lets remove one, and make sure it gets removed remotely.
	idmap := mappedStorage.GetMap()
	for i, _ := range idmap {
		err = destination.RemoveFromMap([]uint64{i})
		assert.NoError(t, err)
		err = mappedStorage.RemoveBlock(i)
		assert.NoError(t, err)
		break
	}

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	// Make sure the maps are the same...
	srcMap := mappedStorage.GetMap()
	destMap := destMappedStorage.GetMap()
	assert.Equal(t, srcMap, destMap)

	migMetrics := mig.GetMetrics()
	fmt.Printf("Migration total migrated blocks %d\n", migMetrics.TotalMigratedBlocks)
}
