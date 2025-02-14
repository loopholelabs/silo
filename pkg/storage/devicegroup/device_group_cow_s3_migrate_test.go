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
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDeviceGroupCowS3(t *testing.T, log types.Logger) *DeviceGroup {
	MinioPort := testutils.SetupMinio(t.Cleanup)

	var testCowS3DeviceSchema = []*config.DeviceSchema{
		{
			Name:      "test1",
			Size:      "8m",
			System:    "file",
			BlockSize: "64k",
			//	Expose:    true,
			Location: "test_data/test1",
		},

		{
			Name:      "test2",
			Size:      "16m",
			System:    "file",
			BlockSize: "64k",
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

	// Not ready yet
	doSync := true

	if doSync {
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
		testCowS3DeviceSchema[1].Sync = sync

		err := sources.CreateBucket(sync.Secure, sync.Endpoint, sync.AccessKey, sync.SecretKey, sync.Bucket)
		assert.NoError(t, err)
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
	dg, err := NewFromSchema("test-instance", testCowS3DeviceSchema, false, log, nil)
	assert.NoError(t, err)

	t.Cleanup(func() {
		err = dg.CloseAll()
		assert.NoError(t, err)

		os.RemoveAll("test_data")
	})

	return dg
}

func TestDeviceGroupCowS3Migrate(t *testing.T) {
	log := logging.New(logging.Zerolog, "silo", os.Stdout)
	//	log.SetLevel(types.TraceLevel)

	dg := setupDeviceGroupCowS3(t, log)
	if dg == nil {
		return
	}

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

	// Lets write some data, which will get written to both S3, and to the CoW overlay.
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
		dg2, err = NewFromProtocol(ctx, "test_instance", prDest, tweak, nil, cdh, log, nil)
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

	// Make sure we are tracking everything...
	dit := dg.GetDeviceInformationByName("test2")
	tMetrics := dit.DirtyRemote.GetMetrics()

	assert.Equal(t, int(tMetrics.TrackingBlocks), dit.NumBlocks)

	// Do a new write to test2, and then dirtyloop. It can't go to S3, since s3.sync will be stopped here.
	// Lets write some data, which will get written to both S3, and to the CoW overlay.
	test2prov := dit.Volatility
	for _, offset := range []int64{10000, 400000} {
		_, err := rand.Read(buff)
		assert.NoError(t, err)
		_, err = test2prov.WriteAt(buff, offset)
		assert.NoError(t, err)
	}

	hooks := &MigrateDirtyHooks{
		PreGetDirty: func(name string) error {
			return nil
		},
		PostGetDirty: func(name string, blocks []uint) (bool, error) {
			return len(blocks) > 0, nil
		},
		PostMigrateDirty: func(name string, blocks []uint) (bool, error) {
			return true, nil
		},
		Completed: func(name string) {
		},
	}

	// Migrate the dirty data. This will go p2p.
	err = dg.MigrateDirty(hooks)
	assert.NoError(t, err)

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

	// Check metrics
	di := dg2.GetDeviceInformationByName("test2")
	metrics := di.From.GetMetrics()
	fmt.Printf("From metrics 'test2' - AvailableP2P:%v AvailableAltSources:%v\n", metrics.AvailableP2P, metrics.AvailableAltSources)

	// It should have got some of the data from S3, and some from P2P.
	assert.Greater(t, len(metrics.AvailableP2P), 0)
	assert.Greater(t, len(metrics.AvailableAltSources), 0)

}
