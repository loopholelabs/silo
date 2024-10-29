package expose

import (
	crand "crypto/rand"
	"fmt"
	"os"
	"os/user"
	"syscall"
	"testing"
	"time"

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
	changedData := 1024 * 1024 * 900
	_, err = crand.Read(mmdata1[:changedData])
	assert.NoError(t, err)

	// Read the soft dirty memory
	ctime := time.Now()
	nbytes, err := pm.readSoftDirtyMemory(mem_start, mem_end, provCheck)
	dtime := time.Since(ctime)
	assert.NoError(t, err)
	mbPerMs := float64(nbytes) / float64(1024*1024*dtime.Milliseconds())
	fmt.Printf("Read %d bytes in %dms at %.2fMB/ms\n", nbytes, dtime.Milliseconds(), mbPerMs)
	/*
		// Reset
		fmt.Printf("Clearing soft_dirty flags\n")
		err = os.WriteFile(fmt.Sprintf("/proc/%d/clear_refs", pid), []byte("4"), 0666)
		assert.NoError(t, err)

		// Change something
		mmdata1[0] = 57

		// Retry
		// Read the soft dirty memory
		nbytes, err = pm.readSoftDirtyMemory(mem_start, mem_end, provCheck)
		assert.NoError(t, err)
		fmt.Printf("RETRY Read %d bytes\n", nbytes)

		// Retry...
	*/
	// This should push all changes to prov
	msync_ctime := time.Now()
	err = unix.Msync(mmdata1, unix.MS_SYNC)
	msync_dtime := time.Since(msync_ctime)
	assert.NoError(t, err)

	fmt.Printf("TIME read %dms msync %dms\n", dtime.Milliseconds(), msync_dtime.Milliseconds())

	equal, err := storage.Equals(provCheck, prov, 64*1024)
	assert.NoError(t, err)
	assert.True(t, equal)
}
