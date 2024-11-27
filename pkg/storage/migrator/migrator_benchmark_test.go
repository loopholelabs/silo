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
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

func BenchmarkMigration(mb *testing.B) {

	size := 4 * 1024 * 1024
	blockSize := 64 * 1024
	numBlocks := (size + blockSize - 1) / blockSize

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

	orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
	orderer.AddAll()

	destStorage := sources.NewMemoryStorage(size)

	conf := NewConfig().WithBlockSize(blockSize)
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
		err = mig.Migrate(numBlocks)
		if err != nil {
			panic(err)
		}

		err = mig.WaitForCompletion()
		if err != nil {
			panic(err)
		}
	}
}

type testConfig struct {
	numPipes    int
	concurrency int
	blockSize   int
	name        string
	shardSize   int
	compress    bool
}

func BenchmarkMigrationPipe(mb *testing.B) {

	size := 15 * 1024 * 1024
	tests := []testConfig{
		{name: "32-concurrency", numPipes: 1, concurrency: 32, blockSize: 64 * 1024, shardSize: 64 * 1024, compress: false},
		{name: "128-concurrency", numPipes: 1, concurrency: 128, blockSize: 64 * 1024, shardSize: 64 * 1024, compress: false},
		{name: "max-concurrency", numPipes: 1, concurrency: 1000000, blockSize: 64 * 1024, shardSize: 64 * 1024, compress: false},

		{name: "1-pipe", numPipes: 1, concurrency: 1000000, blockSize: 64 * 1024, shardSize: 64 * 1024, compress: false},
		{name: "4-pipes", numPipes: 4, concurrency: 1000000, blockSize: 64 * 1024, shardSize: 64 * 1024, compress: false},
		{name: "32-pipes", numPipes: 32, concurrency: 1000000, blockSize: 64 * 1024, shardSize: 64 * 1024, compress: false},

		{name: "4k-blocks", numPipes: 32, concurrency: 1000000, blockSize: 4096, shardSize: 64 * 1024, compress: false},
		{name: "64k-blocks", numPipes: 32, concurrency: 1000000, blockSize: 64 * 1024, shardSize: 64 * 1024, compress: false},
		{name: "256k-blocks", numPipes: 32, concurrency: 1000000, blockSize: 256 * 1024, shardSize: 64 * 1024, compress: false},

		{name: "no-sharding", numPipes: 32, concurrency: 1000000, blockSize: 256 * 1024, shardSize: size, compress: false},
		{name: "4k-shards", numPipes: 32, concurrency: 1000000, blockSize: 256 * 1024, shardSize: 4 * 1024, compress: false},
		{name: "64k-shards", numPipes: 32, concurrency: 1000000, blockSize: 256 * 1024, shardSize: 64 * 1024, compress: false},
		{name: "256k-shards", numPipes: 32, concurrency: 1000000, blockSize: 256 * 1024, shardSize: 256 * 1024, compress: false},

		{name: "no-compress", numPipes: 32, concurrency: 1000000, blockSize: 256 * 1024, shardSize: 256 * 1024, compress: false},
		{name: "compress", numPipes: 32, concurrency: 1000000, blockSize: 256 * 1024, shardSize: 256 * 1024, compress: true},
	}

	for _, testconf := range tests {

		mb.Run(testconf.name, func(b *testing.B) {
			blockSize := testconf.blockSize
			numBlocks := (size + blockSize - 1) / blockSize

			sourceStorageMem := sources.NewMemoryStorage(size)
			sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
			sourceStorage := modules.NewLockable(sourceDirtyLocal)

			// Set up some data here. NB This is v nicely compressable
			buffer := make([]byte, size)
			for i := 0; i < size; i++ {
				buffer[i] = 9
			}

			_, err := sourceStorage.WriteAt(buffer, 0)
			if err != nil {
				panic(err)
			}

			orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
			orderer.AddAll()

			var destStorage storage.Provider

			num := testconf.numPipes

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

			initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
				destStorageFactory := func(di *packets.DevInfo) storage.Provider {
					// Do some sharding here...
					cr := func(_ int, size int) (storage.Provider, error) {
						mem := sources.NewMemoryStorage(size)
						// s := modules.NewArtificialLatency(mem, 5*time.Millisecond, 1*time.Nanosecond, 5*time.Millisecond, 1*time.Nanosecond)
						return mem, nil
					}
					destStorage, err = modules.NewShardedStorage(int(di.Size), testconf.shardSize, cr)

					return destStorage
				}

				// Pipe from the protocol to destWaiting
				destFrom := protocol.NewFromProtocol(ctx, dev, destStorageFactory, p)
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

			prSourceRW := protocol.NewRW(context.TODO(), readers1, writers2, nil)
			prDestRW := protocol.NewRW(context.TODO(), readers2, writers1, initDev)

			go func() {
				_ = prSourceRW.Handle()
			}()
			go func() {
				_ = prDestRW.Handle()
			}()

			prSource := protocol.NewTestProtocolBandwidth(prSourceRW, 1024*1024*1024) // 1GB/sec
			prDest := protocol.NewTestProtocolBandwidth(prDestRW, 1024*1024*1024)     // 1GB/sec

			// Make sure new devs get given the latency/bandwidth protocol...

			prDestRW.SetNewDevProtocol(prDest)

			// Pipe a destination to the protocol
			destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

			if testconf.compress {
				destination.SetCompression(true)
			}

			err = destination.SendDevInfo("test", uint32(blockSize), "")
			if err != nil {
				panic(err)
			}

			conf := NewConfig().WithBlockSize(blockSize)
			conf.LockerHandler = sourceStorage.Lock
			conf.UnlockerHandler = sourceStorage.Unlock
			conf.Concurrency = map[int]int{
				storage.BlockTypeAny:      testconf.concurrency,
				storage.BlockTypeStandard: testconf.concurrency,
				storage.BlockTypeDirty:    testconf.concurrency,
				storage.BlockTypePriority: testconf.concurrency,
			}

			mig, err := NewMigrator(sourceDirtyRemote,
				destination,
				orderer,
				conf)

			if err != nil {
				panic(err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))
			//			b.ReportAllocs()

			//			ctime := time.Now()
			// Migrate some number of times...
			for i := 0; i < b.N; i++ {
				orderer.AddAll()
				err = mig.Migrate(numBlocks)
				if err != nil {
					panic(err)
				}

				err = mig.WaitForCompletion()
				if err != nil {
					panic(err)
				}
			}
			//			duration := time.Since(ctime)
			//			fmt.Printf("Completed %d migrations in %dms - avg %dms\n", b.N, duration.Milliseconds(), int64(duration.Milliseconds())/int64(b.N))
		})

	}
}
