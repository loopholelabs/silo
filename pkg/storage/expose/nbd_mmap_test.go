package expose

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"
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

	var mem_start uint64
	var mem_end uint64

	// Look at proc
	pid := os.Getpid()
	maps, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", pid))
	assert.NoError(t, err)
	lines := strings.Split(string(maps), "\n")
	for _, l := range lines {
		data := strings.Fields(l)
		if len(data) == 6 && data[5] == fmt.Sprintf("/dev/nbd%d", dev1.device_index) {
			fmt.Printf("-- %s\n", l)
			mems := strings.Split(data[0], "-")
			mem_start, err = strconv.ParseUint(mems[0], 16, 64)
			assert.NoError(t, err)
			mem_end, err = strconv.ParseUint(mems[1], 16, 64)
			assert.NoError(t, err)
		}
	}
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
	err = readDirtyProcMem(pid, mem_start, mem_end, provCheckMetrics)
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

// checkProcMapped
func checkProcMapped(pid int, dev string) (int64, error) {
	smaps, err := os.ReadFile(fmt.Sprintf("/proc/%d/smaps", pid))
	if err != nil {
		return 0, err
	}

	// Look through for the data we need...
	lines := strings.Split(string(smaps), "\n")
	lp := 0
	for {
		// Read the header...
		headerData := strings.Fields(lines[lp])
		if len(headerData) == 6 && headerData[5] == dev {
			// This is the one we need...
			for i := 0; i < 26; i++ {
				v := strings.Fields(lines[lp+i])
				if v[0] == "Private_Dirty:" {
					priv, err := strconv.ParseInt(v[1], 10, 64)
					if err != nil {
						return 0, err
					}
					return priv * 1024, nil // k
				}
			}
		} else {
			lp += 26
		}
	}
}

const PAGE_SHIFT = 12 // 4096 page size

const FLAG_PRESENT = 1 << 63
const FLAG_SWAPPED = 1 << 62
const FLAG_SOFT_DIRTY = 1 << 55
const MASK_PFN = (1 << 55) - 1

const KFLAG_DIRTY = 1 << 4
const KFLAG_MMAP = 1 << 11

// Read pagemap file to get dirty pages...
func readPagemap(pid int, addr_start uint64, addr_end uint64) error {
	fmt.Printf("Checking data from %016x - %016x\n", addr_start, addr_end)

	num_soft_dirty := 0
	num_k_dirty := 0
	num_k_mmap := 0

	memf, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pid), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer memf.Close()

	kf, err := os.OpenFile("/proc/kpageflags", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer kf.Close()

	f, err := os.OpenFile(fmt.Sprintf("/proc/%d/pagemap", pid), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	// seek, and read
	pos := int64((addr_start >> PAGE_SHIFT) << 3)
	_, err = f.Seek(pos, io.SeekStart)
	if err != nil {
		return err
	}

	data := make([]byte, 8)
	kdata := make([]byte, 8)
	dataBuffer := make([]byte, 4096)
	for xx := addr_start; xx < addr_end; xx += 4096 {
		_, err = f.Read(data)
		if err != nil {
			return err
		}

		val := binary.LittleEndian.Uint64(data)
		if (val & FLAG_PRESENT) == FLAG_PRESENT {
			if (val & FLAG_SOFT_DIRTY) == FLAG_SOFT_DIRTY {
				num_soft_dirty++
				// Read the data block by block for now
				n, err := memf.ReadAt(dataBuffer, int64(xx))
				if err != nil || n != 4096 {
					return err
				}
				//fmt.Printf("READ %016x %d - %v - %x\n", xx, n, err, dataBuffer[:16])
			}
			pfn := val & MASK_PFN
			// Lookup in /proc/kpageflags
			kpos := int64(pfn << 3)
			_, err = kf.Seek(kpos, io.SeekStart)
			if err != nil {
				return err
			}
			_, err = kf.Read(kdata)
			if err != nil {
				return err
			}

			kval := binary.LittleEndian.Uint64(kdata)

			if (kval & KFLAG_DIRTY) == KFLAG_DIRTY {
				num_k_dirty++
			}
			if (kval & KFLAG_MMAP) == KFLAG_MMAP {
				num_k_mmap++
			}
		}
	}

	fmt.Printf("Found %d soft_dirty pages %d k_dirty %d k_mmap\n", num_soft_dirty, num_k_dirty, num_k_mmap)
	return nil
}

func readDirtyProcMem(pid int, addr_start uint64, addr_end uint64, prov storage.StorageProvider) error {
	memf, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pid), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer memf.Close()

	f, err := os.OpenFile(fmt.Sprintf("/proc/%d/pagemap", pid), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	// seek, and read
	pos := int64((addr_start >> PAGE_SHIFT) << 3)
	_, err = f.Seek(pos, io.SeekStart)
	if err != nil {
		return err
	}

	data := make([]byte, 8)
	dataBuffer := make([]byte, 4*1024*1024) // Max read size

	copyData := func(start uint64, end uint64) error {
		length := end - start
		_, err := memf.ReadAt(dataBuffer[:length], int64(start))
		if err != nil {
			return err
		}
		// NB here we adjust for the start of memory
		_, err = prov.WriteAt(dataBuffer[:length], int64(start-addr_start))
		return err
	}

	currentStart := uint64(0)
	currentEnd := uint64(0)

	for xx := addr_start; xx < addr_end; xx += 4096 {
		_, err = f.Read(data)
		if err != nil {
			return err
		}

		val := binary.LittleEndian.Uint64(data)
		if (val & FLAG_PRESENT) == FLAG_PRESENT {
			if (val & FLAG_SOFT_DIRTY) == FLAG_SOFT_DIRTY {
				if currentEnd == xx {
					if currentEnd-currentStart+4096 > uint64(len(dataBuffer)) {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return err
						}

						currentStart = xx
						currentEnd = xx + 4096
					} else {
						currentEnd = xx + 4096
					}
				} else {
					if currentEnd != 0 {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return err
						}
					}
					currentStart = xx
					currentEnd = xx + 4096
				}
			}
		}
	}

	if currentEnd != 0 {
		err = copyData(currentStart, currentEnd)
		if err != nil {
			return err
		}
	}

	return nil
}
