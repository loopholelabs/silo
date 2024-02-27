package expose

import (
	"fmt"
	"os"
	"os/user"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestNBDNLDevice(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	var n *ExposedStorageNBDNL
	defer func() {
		fmt.Printf("Shutting down properly...\n")
		err := n.Shutdown()
		assert.NoError(t, err)
		fmt.Printf("Shutdown complete\n")
	}()

	size := 4096 * 1024 * 1024
	prov := sources.NewMemoryStorage(size)

	n = NewExposedStorageNBDNL(prov, 1, 0, uint64(size), 4096)

	err = n.Handle()
	assert.NoError(t, err)

	fmt.Printf("WaitReady...\n")
	n.WaitReady()

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", n.DevIndex), os.O_RDWR, 0666)
	assert.NoError(t, err)

	// Try doing a read...
	off := 12
	buffer := make([]byte, 4096)
	num, err := devfile.ReadAt(buffer, int64(off))
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), num)
	devfile.Close()

}
