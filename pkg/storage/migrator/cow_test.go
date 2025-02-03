package migrator_test

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

const testCowDir = "test_migrate_cow"

func setupCowDevice(t *testing.T) (storage.Provider, int, []byte) {
	err := os.Mkdir(testCowDir, 0777)
	assert.NoError(t, err)

	t.Cleanup(func() {
		err := os.RemoveAll(testCowDir)
		assert.NoError(t, err)
	})

	ds := &config.DeviceSchema{
		Name:      "test",
		Size:      "10m",
		System:    "sparsefile",
		BlockSize: "64k",
		Expose:    false,
		Location:  path.Join(testCowDir, "test_overlay"),
		ROSource: &config.DeviceSchema{
			Name:      path.Join(testCowDir, "test_state"),
			Size:      "10m",
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
	for _, offset := range []int64{0, 10 * 1024, 100 * 1024} {
		chgData := make([]byte, 4*1024)
		_, err = crand.Read(chgData)
		assert.NoError(t, err)
		_, err = prov.WriteAt(chgData, offset)
		assert.NoError(t, err)
	}

	return prov, blockSize, baseData
}

func TestCowGetBase(t *testing.T) {
	prov, _, baseData := setupCowDevice(t)

	erd := storage.SendSiloEvent(prov, storage.EventType("getbase"), nil)
	// Check it returns the base provider...
	assert.Equal(t, 1, len(erd))

	baseprov := erd[0].(storage.Provider)

	provBuffer := make([]byte, prov.Size())
	_, err := prov.ReadAt(provBuffer, 0)
	assert.NoError(t, err)

	// The base data and provider data shouldn't be the same...
	assert.NotEqual(t, baseData, provBuffer)

	baseBuffer := make([]byte, baseprov.Size())
	_, err = baseprov.ReadAt(baseBuffer, 0)
	assert.NoError(t, err)

	// The base data should be as we expect
	assert.Equal(t, baseData, baseBuffer)

}

func TestMigratorCow(t *testing.T) {
	prov, blockSize, _ := setupCowDevice(t)

	_, sourceDirtyRemote := dirtytracker.NewDirtyTracker(prov, blockSize)

	numBlocks := (int(prov.Size()) + blockSize - 1) / blockSize

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

	destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

	err := destination.SendDevInfo("test", uint32(blockSize), "")
	assert.NoError(t, err)

	conf := migrator.NewConfig().WithBlockSize(blockSize)

	mig, err := migrator.NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(numBlocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed.
	eq, err := storage.Equals(prov, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	srcDataSent := prSource.GetMetrics().DataSent
	srcDataRecv := prSource.GetMetrics().DataRecv

	fmt.Printf("Transfer [device size %d] transfer bytes: %d sent, %d recv\n", prov.Size(), srcDataSent, srcDataRecv)

	assert.Less(t, srcDataSent, prov.Size())

	destMetrics := destination.GetMetrics()

	fmt.Printf("Sent %d WriteAt     %d bytes\n", destMetrics.SentWriteAt, destMetrics.SentWriteAtBytes)
	fmt.Printf("Sent %d WriteAtComp %d bytes\n", destMetrics.SentWriteAtComp, destMetrics.SentWriteAtCompBytes)
	fmt.Printf("Sent %d WriteAtHash %d bytes\n", destMetrics.SentWriteAtHash, destMetrics.SentWriteAtHashBytes)
}
