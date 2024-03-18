package migrator

import (
	"context"
	"io"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

func BenchmarkMigration(mb *testing.B) {

	size := 4 * 1024 * 1024
	blockSize := 64 * 1024
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	_, err := sourceStorage.WriteAt(buffer, 0)
	if err != nil {
		panic(err)
	}

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	destStorage := sources.NewMemoryStorage(size)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	if err != nil {
		panic(err)
	}

	mb.ResetTimer()
	mb.SetBytes(int64(size))
	mb.ReportAllocs()

	// Migrate some number of times...
	for i := 0; i < mb.N; i++ {
		orderer.AddAll()
		mig.Migrate(num_blocks)

		err = mig.WaitForCompletion()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMigrationPipe(mb *testing.B) {

	size := 4 * 1024 * 1024
	blockSize := 64 * 1024
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	for i := 0; i < size; i++ {
		buffer[i] = 9
	}

	_, err := sourceStorage.WriteAt(buffer, 0)
	if err != nil {
		panic(err)
	}

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	var destStorage storage.StorageProvider

	num := 32

	readers1 := make([]io.Reader, 0)
	readers2 := make([]io.Reader, 0)
	writers1 := make([]io.Writer, 0)
	writers2 := make([]io.Writer, 0)

	for i := 0; i < num; i++ {
		r1, w1 := io.Pipe()
		r2, w2 := io.Pipe()
		readers1 = append(readers1, r1)
		writers1 = append(writers1, w1)

		readers2 = append(readers2, r2)
		writers2 = append(writers2, w2)
	}

	initDev := func(p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *protocol.DevInfo) storage.StorageProvider {
			destStorage = sources.NewMemoryStorage(int(di.Size))
			return destStorage
		}

		// Pipe from the protocol to destWaiting
		destFrom := protocol.NewFromProtocol(dev, destStorageFactory, p)
		ctx := context.TODO()
		go destFrom.HandleSend(ctx)
		go destFrom.HandleReadAt()
		go destFrom.HandleWriteAt()
		go destFrom.HandleDevInfo()
	}

	prSource := protocol.NewProtocolRW(context.TODO(), readers1, writers2, nil)
	prDest := protocol.NewProtocolRW(context.TODO(), readers2, writers1, initDev)

	go prSource.Handle()
	go prDest.Handle()

	// Pipe a destination to the protocol
	destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

	destination.SendDevInfo("test", uint32(blockSize))

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.LockerHandler = sourceStorage.Lock
	conf.UnlockerHandler = sourceStorage.Unlock

	mig, err := NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	if err != nil {
		panic(err)
	}

	mb.ResetTimer()
	mb.SetBytes(int64(size))
	mb.ReportAllocs()

	// Migrate some number of times...
	for i := 0; i < mb.N; i++ {
		orderer.AddAll()
		mig.Migrate(num_blocks)

		err = mig.WaitForCompletion()
		if err != nil {
			panic(err)
		}
	}
}
