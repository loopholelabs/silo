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
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

func setupDevTest(t *testing.T, size int) (*ExposedStorageNBDNL, storage.StorageProvider, []byte) {
	prov := sources.NewMemoryStorage(size)
	provMetrics := modules.NewMetrics(prov)

	n := NewExposedStorageNBDNL(provMetrics, 8, 0, uint64(size), 4096, true)

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
		provMetrics.ShowStats("dev")

		err = syscall.Munmap(mmdata)
		assert.NoError(t, err)

		err = f.Close()
		assert.NoError(t, err)

		err := n.Shutdown()
		assert.NoError(t, err)
	})

	return n, prov, mmdata
}

func TestNBDNLDeviceMmap(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	size := 1024 * 1024 * 1024 * 2
	dev1, prov, mmdata1 := setupDevTest(t, size)

	provCheck := sources.NewMemoryStorage(size)
	provCheckMetrics := modules.NewMetrics(provCheck)

	pid := os.Getpid()
	pm := NewProcessMemory(pid)

	mem_start, mem_end, err := pm.getMemoryRange(fmt.Sprintf("/dev/nbd%d", dev1.device_index))
	assert.NoError(t, err)
	fmt.Printf("MEM IS %d - %d\n", mem_start, mem_end)

	ctime := time.Now()

	totalData := 0
	timeWrites := time.Duration(0)
	timeSyncs := time.Duration(0)
	/*
		maxChangeData := 1024 * 1024 * 64

		doMsync := false
		doWrites := true

		for i := 0; i < 10; i++ {
			if doWrites {
				offset := rand.Intn(size - 1)
				maxLen := size - offset
				if maxLen > maxChangeData {
					maxLen = maxChangeData
				}
				length := rand.Intn(maxLen)

				startWrites := time.Now()
				crand.Read(mmdata1[offset : offset+length])
				timeWrites += time.Since(startWrites)
				//		fmt.Printf("Write took %dms with %d data\n", time.Since(start_writes).Milliseconds(), length)
				totalData += length
			}

			// Check proc stuff here...
			dirty, err := checkProcMapped(pid, fmt.Sprintf("/dev/nbd%d", dev1.device_index))
			assert.NoError(t, err)
			fmt.Printf("Dirty data %d\n", dirty)

			if doMsync {
				// Could be MS_ASYNC
				startMsync := time.Now()
				err = unix.Msync(mmdata1, unix.MS_SYNC)
				timeSyncs += time.Since(startMsync)
				//		fmt.Printf("Msync took %dms with %d data\n", time.Since(start_msync).Milliseconds(), length)
				assert.NoError(t, err)
			}
		}
	*/

	changedData := 1024 * 1024 * 1800
	_, err = crand.Read(mmdata1[:changedData])
	assert.NoError(t, err)

	// Find which pages are dirty...
	startReadPagemap := time.Now()
	//err = readPagemap(pid, mem_start, mem_end)
	err = pm.readDirtyMemory(mem_start, mem_end, provCheckMetrics)
	timeReadPagemap := time.Since(startReadPagemap)
	assert.NoError(t, err)
	fmt.Printf("Time read pagemap took %dms\n", timeReadPagemap.Milliseconds())

	provCheckMetrics.ShowStats("check")

	fmt.Printf("Total data %d bytes, writes %dms syncs %dms\n", totalData, timeWrites.Milliseconds(), timeSyncs.Milliseconds())

	startMsync := time.Now()
	err = unix.Msync(mmdata1, unix.MS_SYNC)
	assert.NoError(t, err)
	timeSyncs += time.Since(startMsync)
	fmt.Printf("Last msync took %dms\n", timeSyncs.Milliseconds())

	write_per_mb := float64(timeWrites.Milliseconds()*1024*1024) / float64(totalData)
	sync_per_mb := float64(timeSyncs.Milliseconds()*1024*1024) / float64(totalData)

	fmt.Printf("TIME_PER_MB Writes %.2f Syncs %.2f\n", write_per_mb, sync_per_mb)
	fmt.Printf("TOTAL TIME %dms\n", time.Since(ctime).Milliseconds())

	equal, err := storage.Equals(provCheck, prov, 64*1024)
	assert.NoError(t, err)
	assert.True(t, equal)
}
