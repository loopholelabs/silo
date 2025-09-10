package swarming

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSwarmingHashMigrate(t *testing.T) {
	log := logging.New(logging.Zerolog, "silo", os.Stdout)
	log.SetLevel(types.TraceLevel)

	dg := setupDeviceGroup(t, log)
	if dg == nil {
		return
	}

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

	// Setup a HashBlockManager, and index blocks in it
	hbm := NewHashBlockManager()
	names := dg.GetAllNames()
	for _, n := range names {
		di := dg.GetDeviceInformationByName(n)
		err := hbm.IndexStorage(di.DirtyRemote, int(di.BlockSize))
		assert.NoError(t, err)
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
	err := dg.StartMigrationTo(prSource, true, packets.CompressionTypeRLE)
	assert.NoError(t, err)

	// Tell it to send no data, we'll get it elsewhere...
	for _, n := range dg.GetAllNames() {
		di := dg.GetDeviceInformationByName(n)
		di.To.SendNoData = true

		// Handle any requests for data by hash here...
		go func() {
			di.To.HandleReadByHash(hbm)
			// NB We'd expect context cancelled here
		}()
	}

	// Make sure the incoming devices were setup completely
	wg.Wait()

	type hashWrite struct {
		offset int64
		length int64
		hash   []byte
	}
	type hashWriteInfo struct {
		writes []*hashWrite
		lock   sync.Mutex
	}

	pendingHashWrites := make(map[string]*hashWriteInfo, 0)

	for _, n := range dg.GetAllNames() {
		pendingHashWrites[n] = &hashWriteInfo{
			writes: make([]*hashWrite, 0),
		}
		di2 := dg2.GetDeviceInformationByName(n)
		di2.From.HashWriteHandler = func(offset int64, length int64, hash []byte, _ packets.DataLocation, _ storage.Provider) {
			fmt.Printf(" ### Incoming hash write %d %d %x\n", offset, length, hash)

			pendingHashWrites[n].lock.Lock()
			defer pendingHashWrites[n].lock.Unlock()
			pendingHashWrites[n].writes = append(pendingHashWrites[n].writes, &hashWrite{
				offset: offset,
				length: length,
				hash:   hash,
			})
		}
	}

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

	fmt.Printf("WaitForCompletion...\n")

	// Make sure all incoming devices are complete
	dg2.WaitForCompletion()

	// Show some metrics...
	pMetrics := prSource.GetMetrics()
	fmt.Printf("Protocol SENT (packets %d data %d urgentPackets %d urgentData %d) RECV (packets %d data %d)\n",
		pMetrics.PacketsSent, pMetrics.DataSent, pMetrics.UrgentPacketsSent, pMetrics.UrgentDataSent,
		pMetrics.PacketsRecv, pMetrics.DataRecv)

	fmt.Printf("Grabbing data from source\n")

	for n, w := range pendingHashWrites {
		di2 := dg2.GetDeviceInformationByName(n)
		for _, ww := range w.writes {
			// Do the remote read, and write it to the device...
			// NB We might want to use prov, since it goes through a writeCombinator

			buffer, err := di2.From.ReadByHash([sha256.Size]byte(ww.hash))
			assert.NoError(t, err)
			// Verify the hash
			hash := sha256.Sum256(buffer)
			assert.Equal(t, hash[:], ww.hash)

			_, err = di2.WaitingCacheRemote.WriteAt(buffer, ww.offset)
			assert.NoError(t, err)
		}
	}

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
	pMetrics = prSource.GetMetrics()
	fmt.Printf("Final Protocol SENT (packets %d data %d urgentPackets %d urgentData %d) RECV (packets %d data %d)\n",
		pMetrics.PacketsSent, pMetrics.DataSent, pMetrics.UrgentPacketsSent, pMetrics.UrgentDataSent,
		pMetrics.PacketsRecv, pMetrics.DataRecv)

	hMetrics := hbm.GetMetrics()
	fmt.Printf("HashBlockManager %d hashes, %d locations, %d adds, %d gets, %d getsNotFound\n",
		hMetrics.StoredHashes, hMetrics.StoredLocations,
		hMetrics.Adds, hbm.metricGets, hbm.metricGetsNotFound,
	)
}
