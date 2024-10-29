package expose

import (
	crand "crypto/rand"
	"fmt"
	"os"
	"os/user"
	"syscall"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

// Setup an exposed nbd device, and mmap it.
func setupDevTest(t *testing.T, size int) (*ExposedStorageNBDNL, storage.StorageProvider, []byte) {
	prov := sources.NewMemoryStorage(size)

	n := NewExposedStorageNBDNL(prov, 8, 0, uint64(size), 4096, true)

	err := n.Init()
	assert.NoError(t, err)

	// mmap the device as well...
	file1 := fmt.Sprintf("/dev/nbd%d", n.device_index)
	f, err := os.OpenFile(file1, os.O_RDWR, 0666)
	assert.NoError(t, err)

	prot := syscall.PROT_READ | syscall.PROT_WRITE
	mmdata, err := syscall.Mmap(int(f.Fd()), 0, int(size), prot, syscall.MAP_SHARED)
	assert.NoError(t, err)

	// Perform cleanup.
	t.Cleanup(func() {
		err = syscall.Munmap(mmdata)
		assert.NoError(t, err)

		err = f.Close()
		assert.NoError(t, err)

		err := n.Shutdown()
		assert.NoError(t, err)
	})

	return n, prov, mmdata
}

func TestProcessMemory(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	size := 1024 * 1024 * 1024
	dev1, prov, mmdata1 := setupDevTest(t, size)

	// Setup a separate storage to check against
	provCheck := sources.NewMemoryStorage(size)

	pid := os.Getpid()
	pm := NewProcessMemory(pid)

	mem_start, mem_end, err := pm.getMemoryRange(fmt.Sprintf("/dev/nbd%d", dev1.device_index))
	assert.NoError(t, err)

	// Change some of the data...
	changedData := 1024 * 1024 * 20
	_, err = crand.Read(mmdata1[:changedData])
	assert.NoError(t, err)

	// Read the soft dirty memory
	err = pm.readSoftDirtyMemory(mem_start, mem_end, provCheck)
	assert.NoError(t, err)

	// This should push all changes to prov
	err = unix.Msync(mmdata1, unix.MS_SYNC)
	assert.NoError(t, err)

	equal, err := storage.Equals(provCheck, prov, 64*1024)
	assert.NoError(t, err)
	assert.True(t, equal)
}
