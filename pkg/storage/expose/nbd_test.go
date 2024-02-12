package expose

import (
	"fmt"
	"os"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestNBDDevice(t *testing.T) {
	var n *ExposedStorageNBD
	dev := "nbd1"
	defer func() {
		fmt.Printf("Shutting down properly...\n")
		err := n.Shutdown()
		assert.NoError(t, err)
		fmt.Printf("Shutdown complete\n")
	}()

	size := 4096 * 1024 * 1024
	prov := sources.NewMemoryStorage(size)

	n = NewExposedStorageNBD(prov, dev, 1, 0, uint64(size), 4096, 0)

	go func() {
		err := n.Handle()
		assert.NoError(t, err)
	}()

	n.WaitReady()

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/%s", dev), os.O_RDWR, 0666)
	assert.NoError(t, err)

	// Try doing a read...
	buffer := make([]byte, 4096)
	num, err := devfile.ReadAt(buffer, 10)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), num)

	devfile.Close()
}
