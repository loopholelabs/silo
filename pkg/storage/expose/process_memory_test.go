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
func setupDevTest(t *testing.T, size int) (*ExposedStorageNBDNL, storage.Provider, []byte) {
	prov := sources.NewMemoryStorage(size)

	n := NewExposedStorageNBDNL(prov, DefaultConfig)

	err := n.Init()
	assert.NoError(t, err)

	// mmap the device as well...
	file1 := fmt.Sprintf("/dev/nbd%d", n.deviceIndex)
	f, err := os.OpenFile(file1, os.O_RDWR, 0666)
	assert.NoError(t, err)

	prot := syscall.PROT_READ | syscall.PROT_WRITE
	mmdata, err := syscall.Mmap(int(f.Fd()), 0, size, prot, syscall.MAP_SHARED)
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

	memStart, memEnd, err := pm.GetMemoryRange(fmt.Sprintf("/dev/nbd%d", dev1.deviceIndex))
	assert.NoError(t, err)

	// Change some of the data...
	changedData := 1024 * 1024 * 1
	_, err = crand.Read(mmdata1[:changedData])
	assert.NoError(t, err)

	// Read the soft dirty memory
	nbytes, err := pm.ReadSoftDirtyMemory(memStart, memEnd, provCheck)
	assert.Equal(t, uint64(PageSize*((changedData+PageSize-1)/PageSize)), nbytes)
	assert.NoError(t, err)

	// Reset soft dirty flags
	err = pm.ClearSoftDirty()
	assert.NoError(t, err)

	// Change something
	mmdata1[0] = 57

	// Retry
	// Read the soft dirty memory
	nbytes, err = pm.ReadSoftDirtyMemory(memStart, memEnd, provCheck)
	assert.NoError(t, err)
	assert.Equal(t, uint64(PageSize), nbytes)

	// This should push all changes to prov
	err = unix.Msync(mmdata1, unix.MS_SYNC)
	assert.NoError(t, err)

	// Make sure the storage agrees...
	equal, err := storage.Equals(provCheck, prov, 64*1024)
	assert.NoError(t, err)
	assert.True(t, equal)
}
