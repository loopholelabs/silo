package device

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestDeviceSync(t *testing.T) {
	PORT_9000 := testutils.SetupMinio(t.Cleanup)

	block_size := 64 * 1024

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
				blockshift = 2
				maxage = "100ms"
				minchanged = 4
				limit = 32
				checkperiod = "100ms"
			}
		}
	}
	`, fmt.Sprintf("localhost:%s", PORT_9000))

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

	num_blocks := (int(prov.Size()) + block_size - 1) / block_size

	buffer := make([]byte, 1024*1024)
	n, err := prov.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1024*1024, n)

	// Tell the sync to start.
	storage.SendEvent(prov, "start_sync", nil)

	// Do some writes here, and wait a little bit...

	wbuffer := make([]byte, 64*1024)
	rand.Read(wbuffer)
	n, err = prov.WriteAt(wbuffer, 64*1024)
	assert.NoError(t, err)
	assert.Equal(t, 64*1024, n)

	// Should be enough time here to migrate the changed block.
	time.Sleep(500 * time.Millisecond)

	// Tell the sync to stop, and return the AlternateSource details.
	sources := storage.SendEvent(prov, "stop_sync", nil)

	locs := make([]string, 0)

	for _, r := range sources {
		alt := r.([]packets.AlternateSource)
		for _, as := range alt {
			fmt.Printf("S3 DATA %s | %d %d | %x\n", as.Location, as.Offset, as.Length, as.Hash)
			locs = append(locs, as.Location)
		}
	}

	// If everything worked, all blocks should be present on S3.
	assert.Equal(t, num_blocks, len(locs))

	prov.Close()
}
