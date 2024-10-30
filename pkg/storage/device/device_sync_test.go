package device

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestDeviceSync(t *testing.T) {
	MinioPort := testutils.SetupMinio(t.Cleanup)

	blockSize := 64 * 1024

	testSyncSchema := fmt.Sprintf(`
	device TestSync {
		system = "file"
		size = "1m"
		blocksize = "64k"
		location = "./testdata/testfile_sync"
		sync {
			secure = false
			accesskey = "silosilo"
			secretkey = "silosilo"
			endpoint = "%s"
			bucket = "silosilo"
			config {
				onlydirty = true
				blockshift = 2
				maxage = "100ms"
				minchanged = 4
				limit = 256
				checkperiod = "100ms"
			}
		}
	}
	`, fmt.Sprintf("localhost:%s", MinioPort))

	s := new(config.SiloSchema)
	err := s.Decode([]byte(testSyncSchema))
	assert.NoError(t, err)
	devs, err := NewDevices(s.Device)
	assert.NoError(t, err)
	t.Cleanup(func() {
		os.Remove("./testdata/testfile_sync")
	})

	assert.Equal(t, 1, len(devs))

	prov := devs["TestSync"].Provider

	numBlocks := (int(prov.Size()) + blockSize - 1) / blockSize

	buffer := make([]byte, 1024*1024)
	_, err = rand.Read(buffer)
	assert.NoError(t, err)
	n, err := prov.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1024*1024, n)

	// Tell the sync to start.
	storage.SendSiloEvent(prov, "sync.start", nil)

	// Do a few write here, and wait a little bit for sync to happen...
	for i := 0; i < numBlocks; i++ {
		wbuffer := make([]byte, blockSize)
		_, err = rand.Read(wbuffer)
		assert.NoError(t, err)
		n, err = prov.WriteAt(wbuffer, int64(i*blockSize))
		assert.NoError(t, err)
		assert.Equal(t, 64*1024, n)
	}

	// Should be enough time here to migrate the changed data blocks, since we have set the config.
	time.Sleep(500 * time.Millisecond)

	// Tell the sync to stop, and return the AlternateSource details.
	asources := storage.SendSiloEvent(prov, "sync.stop", nil)

	locs := make([]string, 0)

	for _, r := range asources {
		alt := r.([]packets.AlternateSource)
		for _, as := range alt {
			// Check the data matches what we have locally...
			buff := make([]byte, as.Length)
			n, err := prov.ReadAt(buff, as.Offset)
			assert.NoError(t, err)
			assert.Equal(t, n, int(as.Length))

			hash := sha256.Sum256(buff)
			assert.Equal(t, hash, as.Hash)

			locs = append(locs, as.Location)
		}
	}

	// If everything worked, all blocks should be present on S3.
	assert.Equal(t, numBlocks, len(locs))

	// Get some statistics
	stats := storage.SendSiloEvent(prov, "sync.status", nil)

	assert.Equal(t, 1, len(stats))
	metrics := stats[0].(*sources.S3Metrics)

	// Do some asserts on the S3Metrics... It should have written each block at least once by now.
	assert.GreaterOrEqual(t, numBlocks, int(metrics.BlocksWCount))

	prov.Close()
}
