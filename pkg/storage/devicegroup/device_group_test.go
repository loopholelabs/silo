package devicegroup

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/loopholelabs/logging"
	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDeviceSchema = []*config.DeviceSchema{
	{
		Name:      "test1",
		Size:      "8m",
		System:    "file",
		BlockSize: "1m",
		//	Expose:    true,
		Location: "test_data/test1",
	},

	{
		Name:      "test2",
		Size:      "16m",
		System:    "file",
		BlockSize: "1m",
		//		Expose:    true,
		Location: "test_data/test2",
		ROSource: &config.DeviceSchema{
			Name:      "test_data/test2state",
			Size:      "16m",
			System:    "file",
			BlockSize: "1m",
			Location:  "test_data/test2base",
		},
		ROSourceShared: true,
	},
}

func setupDeviceGroup(t *testing.T) *DeviceGroup {
	err := os.Mkdir("test_data", 0777)
	assert.NoError(t, err)
	/*
		currentUser, err := user.Current()
		if err != nil {
			panic(err)
		}
		if currentUser.Username != "root" {
			fmt.Printf("Cannot run test unless we are root.\n")
			return nil
		}
	*/
	dg, err := NewFromSchema(testDeviceSchema, false, nil, nil)
	assert.NoError(t, err)

	t.Cleanup(func() {
		err = dg.CloseAll()
		assert.NoError(t, err)

		os.RemoveAll("test_data")
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

	err := dg.StartMigrationTo(pro, true)
	assert.NoError(t, err)

	// Make sure they all got sent correctly...
	_, data, err := pro.WaitForCommand(0, packets.CommandDeviceGroupInfo)
	assert.NoError(t, err)

	dgi, err := packets.DecodeDeviceGroupInfo(data)
	assert.NoError(t, err)

	for index, r := range testDeviceSchema {
		di := dgi.Devices[index+1]

		assert.Equal(t, r.Name, di.Name)
		assert.Equal(t, uint64(r.ByteSize()), di.Size)
		assert.Equal(t, uint32(r.ByteBlockSize()), di.BlockSize)
		assert.Equal(t, string(r.EncodeAsBlock()), di.Schema)
	}
}

func TestDeviceGroupMigrateTo(t *testing.T) {
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
	incomingProviders := make(map[string]storage.Provider)

	prSource := protocol.NewRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewRW(ctx, []io.Reader{r2}, []io.Writer{w1}, nil)

	go func() {
		// This is our control channel, and we're expecting a DeviceGroupInfo
		_, dgData, err := prDest.WaitForCommand(0, packets.CommandDeviceGroupInfo)
		assert.NoError(t, err)
		dgi, err := packets.DecodeDeviceGroupInfo(dgData)
		assert.NoError(t, err)

		for index, di := range dgi.Devices {
			destStorageFactory := func(di *packets.DevInfo) storage.Provider {
				store := sources.NewMemoryStorage(int(di.Size))
				incomingLock.Lock()
				incomingProviders[di.Name] = store
				incomingLock.Unlock()
				return store
			}

			from := protocol.NewFromProtocol(ctx, uint32(index), destStorageFactory, prDest)
			err = from.SetDevInfo(di)
			assert.NoError(t, err)
			go func() {
				err := from.HandleReadAt()
				assert.ErrorIs(t, err, context.Canceled)
			}()
			go func() {
				err := from.HandleWriteAt()
				assert.ErrorIs(t, err, context.Canceled)
			}()
			go func() {
				err := from.HandleDirtyList(func(_ []uint) {
				})
				assert.ErrorIs(t, err, context.Canceled)
			}()
		}
	}()

	go func() {
		_ = prSource.Handle()
	}()
	go func() {
		_ = prDest.Handle()
	}()

	// Lets write some data...
	for _, s := range testDeviceSchema {
		prov := dg.GetProviderByName(s.Name)
		assert.NotNil(t, prov)
		buff := make([]byte, prov.Size())
		_, err := rand.Read(buff)
		assert.NoError(t, err)
		_, err = prov.WriteAt(buff, 0)
		assert.NoError(t, err)
	}

	// Send all the dev info...
	err := dg.StartMigrationTo(prSource, true)
	assert.NoError(t, err)

	pHandler := func(_ map[string]*migrator.MigrationProgress) {}

	err = dg.MigrateAll(100, pHandler)
	assert.NoError(t, err)

	// Check the data all got migrated correctly
	for _, s := range testDeviceSchema {
		prov := dg.GetProviderByName(s.Name)
		// Find the correct destProvider...
		destProvider := incomingProviders[s.Name]
		assert.NotNil(t, destProvider)
		eq, err := storage.Equals(prov, destProvider, 1024*1024)
		assert.NoError(t, err)
		assert.True(t, eq)
	}

	cancelfn()
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

	prSource := protocol.NewRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewRW(ctx, []io.Reader{r2}, []io.Writer{w1}, nil)

	var prDone sync.WaitGroup

	prDone.Add(2)
	go func() {
		_ = prSource.Handle()
		prDone.Done()
	}()
	go func() {
		_ = prDest.Handle()
		prDone.Done()
	}()

	// Lets write some data...
	buff := make([]byte, 4096)
	for _, s := range testDeviceSchema {
		prov := dg.GetProviderByName(s.Name)
		if s.Name == "test1" {
			allBuff := make([]byte, prov.Size())
			_, err := rand.Read(allBuff)
			assert.NoError(t, err)
			_, err = prov.WriteAt(allBuff, 0)
			assert.NoError(t, err)
		} else {
			for _, offset := range []int64{10000, 300000, 700000} {
				_, err := rand.Read(buff)
				assert.NoError(t, err)
				_, err = prov.WriteAt(buff, offset)
				assert.NoError(t, err)
			}
		}
	}

	var dg2 *DeviceGroup
	var wg sync.WaitGroup

	// We will tweak schema in recv here so we have separate paths.
	tweak := func(_ int, _ string, schema *config.DeviceSchema) *config.DeviceSchema {
		schema.Location = fmt.Sprintf(schema.Location, ".recv")
		// Tweak overlay if there is one as well...
		if schema.ROSource != nil {
			schema.ROSource.Location = fmt.Sprintf(schema.ROSource.Location, ".recv")
			schema.ROSource.Name = fmt.Sprintf(schema.ROSource.Name, ".recv")
		}
		return schema
	}

	// TransferAuthority
	var tawg sync.WaitGroup
	tawg.Add(1)
	cdh := func(data []byte) {
		assert.Equal(t, []byte("Hello"), data)
		tawg.Done()
	}

	wg.Add(1)
	go func() {
		var err error
		dg2, err = NewFromProtocol(ctx, prDest, tweak, nil, cdh, nil, nil)
		assert.NoError(t, err)
		wg.Done()
	}()

	// Send all the dev info...
	err := dg.StartMigrationTo(prSource, true)
	assert.NoError(t, err)

	// Make sure the incoming devices were setup completely
	wg.Wait()

	// TransferAuthority
	tawg.Add(1)
	time.AfterFunc(100*time.Millisecond, func() {
		dg.SendCustomData([]byte("Hello"))
		tawg.Done()
	})

	pHandler := func(_ map[string]*migrator.MigrationProgress) {}

	err = dg.MigrateAll(100, pHandler)
	assert.NoError(t, err)

	// Make sure authority has been transferred as expected.
	tawg.Wait()

	err = dg.Completed()
	assert.NoError(t, err)

	// Make sure all incoming devices are complete
	dg2.WaitForCompletion()

	// Check the data all got migrated correctly from dg to dg2.
	for _, s := range testDeviceSchema {
		prov := dg.GetProviderByName(s.Name)
		require.NotNil(t, prov)
		destProvider := dg2.GetProviderByName(s.Name)
		require.NotNil(t, destProvider)
		eq, err := storage.Equals(prov, destProvider, 1024*1024)
		assert.NoError(t, err)
		assert.True(t, eq)
	}

	// Cancel context
	cancelfn()

	// Close protocol bits
	prDone.Wait()
	r1.Close()
	w1.Close()
	r2.Close()
	w2.Close()

	// Show some metrics...
	pMetrics := prSource.GetMetrics()
	fmt.Printf("Protocol SENT (packets %d data %d urgentPackets %d urgentData %d) RECV (packets %d data %d)\n",
		pMetrics.PacketsSent, pMetrics.DataSent, pMetrics.UrgentPacketsSent, pMetrics.UrgentDataSent,
		pMetrics.PacketsRecv, pMetrics.DataRecv)
}
