package migrator_test

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"os"
	"path"
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
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
	"github.com/stretchr/testify/assert"
)

const testCowDir = "test_migrate_cow"

func setupCowDevice(t *testing.T, sharedBase bool) (storage.Provider, int, []byte) {
	err := os.Mkdir(testCowDir, 0777)
	assert.NoError(t, err)

	t.Cleanup(func() {
		err := os.RemoveAll(testCowDir)
		assert.NoError(t, err)
	})

	ds := &config.DeviceSchema{
		Name:           "test",
		Size:           "50m",
		System:         "sparsefile",
		BlockSize:      "64k",
		Expose:         false,
		Location:       path.Join(testCowDir, "test_overlay"),
		ROSourceShared: sharedBase,
		ROSource: &config.DeviceSchema{
			Name:      path.Join(testCowDir, "test_state"),
			Size:      "50m",
			System:    "file",
			BlockSize: "64k",
			Expose:    false,
			Location:  path.Join(testCowDir, "test_rosource"),
		},
	}

	// Write some base data
	baseData := make([]byte, ds.ByteSize())
	_, err = crand.Read(baseData)
	assert.NoError(t, err)

	err = os.WriteFile(path.Join(testCowDir, "test_rosource"), baseData, 0666)
	assert.NoError(t, err)

	blockSize := int(ds.ByteBlockSize())

	prov, _, err := device.NewDevice(ds)
	assert.NoError(t, err)

	t.Cleanup(func() {
		prov.Close()
	})

	// Write some changes to the device...
	for _, offset := range []int64{0, 10 * 1024, 400000, 701902} {
		chgData := make([]byte, 4*1024)
		_, err = crand.Read(chgData)
		assert.NoError(t, err)
		_, err = prov.WriteAt(chgData, offset)
		assert.NoError(t, err)
	}

	return prov, blockSize, baseData
}

func TestCowGetBlocks(t *testing.T) {
	prov, _, _ := setupCowDevice(t, true)

	// Setup a dirty tracker
	_, trackRemote := dirtytracker.NewDirtyTracker(prov, 65536)

	blocks := trackRemote.GetUnrequiredBlocks()

	// Check they're being tracked now
	tracking := trackRemote.GetTrackedBlocks()

	expected := make([]uint, 0)
	for v := 0; v < 800; v++ {
		if v != 0 && v != 6 && v != 10 {
			expected = append(expected, uint(v))
		}
	}

	assert.Equal(t, expected, blocks)
	assert.Equal(t, expected, tracking)
}

type migratorCowTest struct {
	name       string
	sharedBase bool
}

func TestMigratorCow(tt *testing.T) {

	for _, v := range []migratorCowTest{
		{name: "standard", sharedBase: false},
		{name: "better", sharedBase: true},
	} {
		tt.Run(v.name, func(t *testing.T) {
			prov, blockSize, _ := setupCowDevice(t, v.sharedBase)

			// Add some metrics
			provMetrics := modules.NewMetrics(prov)

			_, sourceDirtyRemote := dirtytracker.NewDirtyTracker(provMetrics, blockSize)

			numBlocks := (int(provMetrics.Size()) + blockSize - 1) / blockSize

			orderer := blocks.NewAnyBlockOrder(numBlocks, nil)

			// START moving data from sourceStorage to destStorage

			var destStorage storage.Provider
			var destOverlay storage.Provider
			var destFrom *protocol.FromProtocol
			var waitingCacheLocal *waitingcache.Local
			var waitingCacheRemote *waitingcache.Remote

			// Create a simple pipe
			r1, w1 := io.Pipe()
			r2, w2 := io.Pipe()

			// Open the base image
			baseProvider, err := sources.NewFileStorage(path.Join(testCowDir, "test_rosource"), int64(provMetrics.Size()))
			assert.NoError(t, err)

			initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
				destStorageFactory := func(di *packets.DevInfo) storage.Provider {
					var err error
					destOverlay, err = sources.NewFileStorageCreate(path.Join(testCowDir, "test_overlay_dest"), int64(di.Size))
					assert.NoError(t, err)

					destStorage = modules.NewCopyOnWrite(baseProvider, destOverlay, blockSize, true, nil)

					waitingCacheLocal, waitingCacheRemote = waitingcache.NewWaitingCache(destStorage, blockSize)
					return waitingCacheRemote
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

			orderer.AddAll()

			// Pipe a destination to the protocol
			destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

			err = destination.SendDevInfo("test", uint32(blockSize), "")
			assert.NoError(t, err)

			migrateBlocks := numBlocks

			if v.sharedBase {
				// With this, ONLY migrate the blocks we need... For the others, we'll send cmds manually...
				unrequired := sourceDirtyRemote.GetUnrequiredBlocks()
				alreadyBlocks := make([]uint32, 0)
				for _, b := range unrequired {
					orderer.Remove(int(b))
					migrateBlocks--
					alreadyBlocks = append(alreadyBlocks, uint32(b))
				}

				err = destination.SendYouAlreadyHave(uint64(blockSize), alreadyBlocks)
				assert.NoError(t, err)
			}

			conf := migrator.NewConfig().WithBlockSize(blockSize)

			mig, err := migrator.NewMigrator(sourceDirtyRemote,
				destination,
				orderer,
				conf)

			assert.NoError(t, err)

			err = mig.Migrate(migrateBlocks)
			assert.NoError(t, err)

			err = mig.WaitForCompletion()
			assert.NoError(t, err)

			assert.NotNil(t, destStorage)

			// Check how much of the base we had to read
			metrics := provMetrics.GetMetrics()

			fmt.Printf("Provider reads %d (%d bytes) in %dms\n", metrics.ReadOps, metrics.ReadBytes, time.Duration(metrics.ReadTime).Milliseconds())

			// This will end with migration completed. (Go direct to prov here instead of provMetrics)
			eq, err := storage.Equals(prov, destStorage, blockSize)
			assert.NoError(t, err)
			assert.True(t, eq)

			srcDataSent := prSource.GetMetrics().DataSent
			srcDataRecv := prSource.GetMetrics().DataRecv

			fmt.Printf("Transfer [device size %d] transfer bytes: %d sent, %d recv\n", prov.Size(), srcDataSent, srcDataRecv)

			destMetrics := destination.GetMetrics()

			rMetrics := destFrom.GetMetrics()

			fmt.Printf("Recv WriteAt %d | WriteAtComp %d | WriteAtHash %d | WriteAtYouAlreadyHave %d\n",
				rMetrics.RecvWriteAt,
				rMetrics.RecvWriteAtComp,
				rMetrics.RecvWriteAtHash,
				rMetrics.RecvWriteAtYouAlreadyHave)

			fmt.Printf("Sent WriteAt %d (%d bytes) | WriteAtComp %d (%d bytes) | WriteAtHash %d (%d bytes)\n",
				destMetrics.SentWriteAt, destMetrics.SentWriteAtBytes,
				destMetrics.SentWriteAtComp, destMetrics.SentWriteAtCompBytes,
				destMetrics.SentWriteAtHash, destMetrics.SentWriteAtHashBytes,
			)

			waitMetrics := waitingCacheLocal.GetMetrics()

			// The waiting cache should consider ALL blocks present and correct.
			assert.Equal(t, numBlocks, int(waitMetrics.AvailableRemote))
		})
	}
}
