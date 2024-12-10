package devicegroup

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/user"
	"sync"
	"testing"

	"github.com/loopholelabs/logging"
	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

var testDeviceSchema = []*config.DeviceSchema{
	{
		Name:      "test1",
		Size:      "8m",
		System:    "file",
		BlockSize: "1m",
		Expose:    true,
		Location:  "testdev_test1",
	},
	{
		Name:      "test2",
		Size:      "16m",
		System:    "file",
		BlockSize: "1m",
		Expose:    true,
		Location:  "testdev_test2",
	},
}

func setupDeviceGroup(t *testing.T) *DeviceGroup {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return nil
	}

	dg, err := New(testDeviceSchema, nil, nil)
	assert.NoError(t, err)

	t.Cleanup(func() {
		err = dg.CloseAll()
		assert.NoError(t, err)

		os.Remove("testdev_test1")
		os.Remove("testdev_test2")
	})

	return dg
}

func TestDeviceGroupBasic(t *testing.T) {
	dg := setupDeviceGroup(t)
	if dg == nil {
		return
	}
}

func TestDeviceGroupSendDevInfo(t *testing.T) {
	dg := setupDeviceGroup(t)
	if dg == nil {
		return
	}

	pro := protocol.NewMockProtocol(context.TODO())

	err := dg.StartMigrationTo(pro)
	assert.NoError(t, err)

	// Make sure they all got sent correctly...
	for index, r := range testDeviceSchema {
		_, data, err := pro.WaitForCommand(uint32(index), packets.CommandDevInfo)
		assert.NoError(t, err)

		di, err := packets.DecodeDevInfo(data)
		assert.NoError(t, err)

		assert.Equal(t, r.Name, di.Name)
		assert.Equal(t, uint64(r.ByteSize()), di.Size)
		assert.Equal(t, uint32(r.ByteBlockSize()), di.BlockSize)
		assert.Equal(t, string(r.Encode()), di.Schema)
	}
}

func TestDeviceGroupMigrate(t *testing.T) {
	dg := setupDeviceGroup(t)
	if dg == nil {
		return
	}

	log := logging.New(logging.Zerolog, "silo", os.Stdout)
	log.SetLevel(types.TraceLevel)

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	ctx, cancelfn := context.WithCancel(context.TODO())

	var incomingLock sync.Mutex
	incomingProviders := make(map[uint32]storage.Provider)

	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.Provider {
			store := sources.NewMemoryStorage(int(di.Size))
			incomingLock.Lock()
			incomingProviders[dev] = store
			incomingLock.Unlock()
			return store
		}

		from := protocol.NewFromProtocol(ctx, dev, destStorageFactory, p)
		go func() {
			err := from.HandleReadAt()
			assert.ErrorIs(t, err, context.Canceled)
		}()
		go func() {
			err := from.HandleWriteAt()
			assert.ErrorIs(t, err, context.Canceled)
		}()
		go func() {
			err := from.HandleDevInfo()
			assert.NoError(t, err)
		}()
		go func() {
			err := from.HandleDirtyList(func(_ []uint) {
			})
			assert.ErrorIs(t, err, context.Canceled)
		}()
	}

	prSource := protocol.NewRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewRW(ctx, []io.Reader{r2}, []io.Writer{w1}, initDev)

	go func() {
		_ = prSource.Handle()
	}()
	go func() {
		_ = prDest.Handle()
	}()

	// Lets write some data...
	for i := range testDeviceSchema {
		prov := dg.GetProvider(i)
		buff := make([]byte, prov.Size())
		_, err := rand.Read(buff)
		assert.NoError(t, err)
		_, err = prov.WriteAt(buff, 0)
		assert.NoError(t, err)
	}

	// Send all the dev info...
	err := dg.StartMigrationTo(prSource)
	assert.NoError(t, err)

	pHandler := func(_ int, _ *migrator.MigrationProgress) {}

	err = dg.MigrateAll(100, pHandler)
	assert.NoError(t, err)

	// Check the data all got migrated correctly
	for i := range testDeviceSchema {
		prov := dg.GetProvider(i)
		destProvider := incomingProviders[uint32(i)]
		assert.NotNil(t, destProvider)
		eq, err := storage.Equals(prov, destProvider, 1024*1024)
		assert.NoError(t, err)
		assert.True(t, eq)
	}

	cancelfn()
}
