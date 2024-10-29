package expose

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/loopholelabs/silo/pkg/storage"
)

const PAGE_SIZE = 4096
const PAGE_SHIFT = 12

const READ_BUFFER_SIZE = 4 * 1024 * 1024 // 4mb should be fairly fast

// pagemap flags
const PAGEMAP_FLAG_SOFT_DIRTY = 1 << 55
const PAGEMAP_FLAG_EXCLUSIVE = 1 << 56
const PAGEMAP_FLAG_FILEPAGE = 1 << 61
const PAGEMAP_FLAG_SWAPPED = 1 << 62
const PAGEMAP_FLAG_PRESENT = 1 << 63
const PAGEMAP_MASK_PFN = (1 << 55) - 1
const PAGEMAP_MASK_SWAP_TYPE = (1 << 5) - 1
const PAGEMAP_SWAP_OFFSET_SHIFT = 5
const PAGEMAP_MASK_SWAP_OFFSET = (1 << 50) - 1

// kpageflags
const KFLAG_LOCKED = 1
const KFLAG_ERROR = 1 << 1
const KFLAG_REFERENCED = 1 << 2
const KFLAG_UPTODATE = 1 << 3
const KFLAG_DIRTY = 1 << 4
const KFLAG_LRU = 1 << 5
const KFLAG_ACTIVE = 1 << 6
const KFLAG_SLAB = 1 << 7
const KFLAG_WRITEBACK = 1 << 8
const KFLAG_RECLAIM = 1 << 9
const KFLAG_BUDDY = 1 << 10
const KFLAG_MMAP = 1 << 11
const KFLAG_ANON = 1 << 12
const KFLAG_SWAPCACHE = 1 << 13
const KFLAG_SWAPBACKED = 1 << 14
const KFLAG_COMPOUND_HEAD = 1 << 15
const KFLAG_COMPOUND_TAIL = 1 << 16
const KFLAG_HUGE = 1 << 17
const KFLAG_UNEVICTABLE = 1 << 18
const KFLAG_HWPOISON = 1 << 19
const KFLAG_NOPAGE = 1 << 20
const KFLAG_KSM = 1 << 21
const KFLAG_THP = 1 << 22
const KFLAG_BALLOON = 1 << 23
const KFLAG_ZERO_PAGE = 1 << 24
const KFLAG_IDLE = 1 << 25

/**
 * ProcessMemory
 *
 */
type ProcessMemory struct {
	pid int
}

/**
 * Smap
 *
 */
type Smap struct {
	Size            int
	KernelPageSize  int
	MMUPageSize     int
	Rss             int
	Pss             int
	Pss_Dirty       int
	Shared_Clean    int
	Shared_Dirty    int
	Private_Clean   int
	Private_Dirty   int
	Referenced      int
	Anonymous       int
	KSM             int
	LazyFree        int
	AnonHugePages   int
	ShmemPmdMapped  int
	FilePmdMapped   int
	Shared_Hugetlb  int
	Private_Hugetlb int
	Swap            int
	SwapPss         int
	Locked          int
	THPeligible     string
	ProtectionKey   string
	VmFlags         string
}

func NewProcessMemory(pid int) *ProcessMemory {
	return &ProcessMemory{pid: pid}
}

/**
 *
 *
 */
func (pm *ProcessMemory) getMemoryRange(dev string) (uint64, uint64, error) {
	maps, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", pm.pid))
	if err != nil {
		return 0, 0, err
	}
	lines := strings.Split(string(maps), "\n")
	for _, l := range lines {
		data := strings.Fields(l)
		if len(data) == 6 && data[5] == dev {
			mems := strings.Split(data[0], "-")
			mem_start, err := strconv.ParseUint(mems[0], 16, 64)
			if err != nil {
				return 0, 0, err
			}
			mem_end, err := strconv.ParseUint(mems[1], 16, 64)
			if err != nil {
				return 0, 0, err
			}
			return mem_start, mem_end, nil
		}
	}
	return 0, 0, errors.New("device not found in maps")
}

/**
 * getSmap - Get the smap data for a given mapped device.
 *
 */
func (pm *ProcessMemory) getSmap(dev string) (*Smap, error) {
	smaps, err := os.ReadFile(fmt.Sprintf("/proc/%d/smaps", pm.pid))
	if err != nil {
		return nil, err
	}

	smap := &Smap{}

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
				if v[0] == "THPeligible:" {
					smap.THPeligible = v[1]
				} else if v[0] == "ProtectionKey:" {
					smap.ProtectionKey = v[1]
				} else if v[0] == "VmFlags:" {
					smap.VmFlags = v[1]
				} else {
					kvalue, kerr := strconv.ParseInt(v[1], 10, 64)
					if kerr != nil {
						return nil, kerr
					}
					if v[0] == "Size:" {
						smap.Size = int(kvalue) * 1024
					} else if v[0] == "KernelPageSize:" {
						smap.KernelPageSize = int(kvalue) * 1024
					} else if v[0] == "MMUPageSize:" {
						smap.MMUPageSize = int(kvalue) * 1024
					} else if v[0] == "Rss:" {
						smap.Rss = int(kvalue) * 1024
					} else if v[0] == "Pss:" {
						smap.Pss = int(kvalue) * 1024
					} else if v[0] == "Pss_Dirty:" {
						smap.Pss_Dirty = int(kvalue) * 1024
					} else if v[0] == "Shared_Clean:" {
						smap.Shared_Clean = int(kvalue) * 1024
					} else if v[0] == "Shared_Dirty:" {
						smap.Shared_Dirty = int(kvalue) * 1024
					} else if v[0] == "Private_Clean:" {
						smap.Private_Clean = int(kvalue) * 1024
					} else if v[0] == "Private_Dirty:" {
						smap.Private_Dirty = int(kvalue) * 1024
					} else if v[0] == "Referenced:" {
						smap.Referenced = int(kvalue) * 1024
					} else if v[0] == "Anonymous:" {
						smap.Anonymous = int(kvalue) * 1024
					} else if v[0] == "KSM:" {
						smap.KSM = int(kvalue) * 1024
					} else if v[0] == "LazyFree:" {
						smap.LazyFree = int(kvalue) * 1024
					} else if v[0] == "AnonHugePages:" {
						smap.AnonHugePages = int(kvalue) * 1024
					} else if v[0] == "ShmemPmdMapped:" {
						smap.ShmemPmdMapped = int(kvalue) * 1024
					} else if v[0] == "FilePmdMapped:" {
						smap.FilePmdMapped = int(kvalue) * 1024
					} else if v[0] == "Shared_Hugetlb:" {
						smap.Shared_Hugetlb = int(kvalue) * 1024
					} else if v[0] == "Private_Hugetlb:" {
						smap.Private_Hugetlb = int(kvalue) * 1024
					} else if v[0] == "Swap:" {
						smap.Swap = int(kvalue) * 1024
					} else if v[0] == "SwapPss:" {
						smap.SwapPss = int(kvalue) * 1024
					} else if v[0] == "Locked:" {
						smap.Locked = int(kvalue) * 1024
					}
				}
			}
		} else {
			lp += 26
		}
	}
}

/**
 * readSoftDirtyMemory
 *
 */
func (pm *ProcessMemory) readSoftDirtyMemory(addr_start uint64, addr_end uint64, prov storage.StorageProvider) error {
	memf, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pm.pid), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer memf.Close()

	f, err := os.OpenFile(fmt.Sprintf("/proc/%d/pagemap", pm.pid), os.O_RDONLY, 0)
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

	//kdata := make([]byte, 8)
	data := make([]byte, 8)
	dataBuffer := make([]byte, READ_BUFFER_SIZE) // Max read size

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

	for xx := addr_start; xx < addr_end; xx += PAGE_SIZE {
		_, err = f.Read(data)
		if err != nil {
			return err
		}

		val := binary.LittleEndian.Uint64(data)
		if (val & PAGEMAP_FLAG_PRESENT) == PAGEMAP_FLAG_PRESENT {
			if (val & PAGEMAP_FLAG_SOFT_DIRTY) == PAGEMAP_FLAG_SOFT_DIRTY {
				if currentEnd == xx {
					if currentEnd-currentStart+PAGE_SIZE > uint64(len(dataBuffer)) {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return err
						}

						currentStart = xx
						currentEnd = xx + PAGE_SIZE
					} else {
						currentEnd = xx + PAGE_SIZE
					}
				} else {
					if currentEnd != 0 {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return err
						}
					}
					currentStart = xx
					currentEnd = xx + PAGE_SIZE
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

/**
 * readDirtyMemory
 *
 */
func (pm *ProcessMemory) readDirtyMemory(addr_start uint64, addr_end uint64, prov storage.StorageProvider) error {
	memf, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pm.pid), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer memf.Close()

	f, err := os.OpenFile(fmt.Sprintf("/proc/%d/pagemap", pm.pid), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	kf, err := os.OpenFile("/proc/kpageflags", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer kf.Close()

	// seek, and read
	pos := int64((addr_start >> PAGE_SHIFT) << 3)
	_, err = f.Seek(pos, io.SeekStart)
	if err != nil {
		return err
	}

	kdata := make([]byte, 8)
	data := make([]byte, 8)
	dataBuffer := make([]byte, READ_BUFFER_SIZE) // Max read size

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

	for xx := addr_start; xx < addr_end; xx += PAGE_SIZE {
		_, err = f.Read(data)
		if err != nil {
			return err
		}

		val := binary.LittleEndian.Uint64(data)
		if (val & PAGEMAP_FLAG_PRESENT) == PAGEMAP_FLAG_PRESENT {
			pfn := val & PAGEMAP_MASK_PFN
			// Lookup in /proc/kpageflags
			_, err = kf.Seek(int64(pfn<<3), io.SeekStart)
			if err != nil {
				return err
			}
			_, err = kf.Read(kdata)
			if err != nil {
				return err
			}

			kval := binary.LittleEndian.Uint64(kdata)

			if (kval & KFLAG_DIRTY) == KFLAG_DIRTY {
				if currentEnd == xx {
					if currentEnd-currentStart+PAGE_SIZE > uint64(len(dataBuffer)) {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return err
						}

						currentStart = xx
						currentEnd = xx + PAGE_SIZE
					} else {
						currentEnd = xx + PAGE_SIZE
					}
				} else {
					if currentEnd != 0 {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return err
						}
					}
					currentStart = xx
					currentEnd = xx + PAGE_SIZE
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

/**
 * readAllMemory
 *
 */
func (pm *ProcessMemory) readAllMemory(addr_start uint64, addr_end uint64, prov storage.StorageProvider) error {
	memf, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pm.pid), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer memf.Close()

	dataBuffer := make([]byte, READ_BUFFER_SIZE) // Max read size

	for xx := addr_start; xx < addr_end; xx += uint64(len(dataBuffer)) {
		length := uint64(len(dataBuffer))
		if xx+length >= addr_end {
			length = addr_end - xx
		}
		_, err := memf.ReadAt(dataBuffer[:length], int64(xx))
		if err != nil {
			return err
		}
		// NB here we adjust for the start of memory
		_, err = prov.WriteAt(dataBuffer[:length], int64(xx-addr_start))
		if err != nil {
			return err
		}
	}

	return nil
}
