package swarming

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
	"github.com/loopholelabs/silo/pkg/storage/devicegroup"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDeviceSchema = []*config.DeviceSchema{
	{
		Name:      "test1",
		Size:      "8m",
		System:    "file",
		BlockSize: "1m",
		Location:  "test_data/test1",
		Migration: &config.MigrationConfigSchema{
			AnyOrder: true,
		},
	},

	{
		Name:      "test2",
		Size:      "16m",
		System:    "sparsefile",
		BlockSize: "1m",
		Location:  "test_data/test2",
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

func setupDeviceGroup(t *testing.T, log types.Logger) *devicegroup.DeviceGroup {
	MinioPort := testutils.SetupMinio(t.Cleanup)

	sync := &config.SyncS3Schema{
		Secure:          false,
		AutoStart:       true,
		AccessKey:       "silosilo",
		SecretKey:       "silosilo",
		Endpoint:        fmt.Sprintf("localhost:%s", MinioPort),
		Bucket:          "silosilo",
		GrabConcurrency: 10,
		Config: &config.SyncConfigSchema{
			OnlyDirty:   true,
			BlockShift:  2,
			MaxAge:      "100ms",
			MinChanged:  4,
			Limit:       256,
			CheckPeriod: "100ms",
			Concurrency: 10,
		},
	}

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

	testDeviceSchema[1].Sync = sync

	dg, err := devicegroup.NewFromSchema("test-instance", testDeviceSchema, false, log, nil)
	assert.NoError(t, err)

	err = sources.CreateBucket(sync.Secure, sync.Endpoint, sync.AccessKey, sync.SecretKey, sync.Bucket)
	assert.NoError(t, err)

	t.Cleanup(func() {
		err = dg.CloseAll()
		assert.NoError(t, err)

		os.RemoveAll("test_data")
	})

	return dg
}

func TestSwarmingMigrate(t *testing.T) {
	log := logging.New(logging.Zerolog, "silo", os.Stdout)
	log.SetLevel(types.TraceLevel)

	dg := setupDeviceGroup(t, log)
	if dg == nil {
		return
	}

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	ctx, cancelfn := context.WithCancel(context.TODO())

	prSource := protocol.NewRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewRW(ctx, []io.Reader{r2}, []io.Writer{w1}, nil)

	// Wrap protocol so we can see what's going on...
	prSourceMim := protocol.NewMim(prSource)
	prSourceMim.PostSendPacket = func(dev uint32, id uint32, data []byte, urgency protocol.Urgency, pid uint32, err error) {
		cmdString := packets.CommandString(data[0])
		if data[0] == packets.CommandWriteAt {
			cmdString = packets.WriteAtType(data[1])
		}
		fmt.Printf(" src-> dev %d id %d data %d urgency %d pid %d err %v cmd %s\n", dev, id, len(data), urgency, pid, err, cmdString)
	}

	prSourceMim.PostSendDeviceGroupInfo = func(dev uint32, id uint32, dgi *packets.DeviceGroupInfo, err error) {
		fmt.Printf(" src-> DeviceGroupInfo %d %d %v\n", dev, id, dgi)
	}

	prSourceMim.PostWaitForPacket = func(dev uint32, id uint32, data []byte, err error) {
		cmdString := packets.CommandString(data[0])
		fmt.Printf(" src-> WaitForPacket dev %d id %d data %d err %v cmd %s\n", dev, id, len(data), err, cmdString)
	}
	prSourceMim.PostWaitForCommand = func(dev uint32, cmd byte, id uint32, data []byte, err error) {
		cmdString := packets.CommandString(cmd)
		fmt.Printf(" src-> WaitForCommand dev %d cmd %s id %d data %d err %v\n", dev, cmdString, id, len(data), err)
	}

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

	// Make sure it has some time to write to S3
	time.Sleep(5 * time.Second)

	var dg2 *devicegroup.DeviceGroup
	var wg sync.WaitGroup

	// We will tweak schema in recv here so we have separate paths.
	tweak := func(_ int, _ string, schema *config.DeviceSchema) *config.DeviceSchema {
		schema.Location = fmt.Sprintf("%s.recv", schema.Location)
		// Tweak overlay if there is one as well...
		if schema.ROSource != nil {
			schema.ROSource.Location = fmt.Sprintf("%s.recv", schema.ROSource.Location)
			schema.ROSource.Name = fmt.Sprintf("%s.recv", schema.ROSource.Name)
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
		dg2, err = devicegroup.NewFromProtocol(ctx, "test_instance", prDest, tweak, nil, cdh, nil, nil)
		assert.NoError(t, err)
		wg.Done()
	}()

	// Send all the dev info...
	err := dg.StartMigrationTo(prSourceMim, true, packets.CompressionTypeRLE)
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

		di := dg.GetDeviceInformationByName(s.Name)
		if s.Name == "test1" {
			assert.Nil(t, di.Volatility)
		} else {
			assert.NotNil(t, di.Volatility)
		}
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
