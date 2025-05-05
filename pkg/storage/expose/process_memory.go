package expose

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

const PageSize = 4096
const PageShift = 12

const ReadBufferSize = 4 * 1024 * 1024 // 4mb should be fairly fast

// pagemap flags
const PagemapFlagSoftDirty = 1 << 55
const PagemapFlagExclusive = 1 << 56
const PagemapFlagFilepage = 1 << 61
const PagemapFlagSwapped = 1 << 62
const PagemapFlagPresent = 1 << 63
const PagemapMaskPfn = (1 << 55) - 1
const PagemapMaskSwapType = (1 << 5) - 1
const PagemapSwapOffsetShift = 5
const PagemapMaskSwapOffset = (1 << 50) - 1

// kpageflags
const KflagLocked = 1
const KflagError = 1 << 1
const KflagReferenced = 1 << 2
const KflagUptodate = 1 << 3
const KflagDirty = 1 << 4
const KflagLru = 1 << 5
const KflagActive = 1 << 6
const KflagSlab = 1 << 7
const KflagWriteback = 1 << 8
const KflagReclaim = 1 << 9
const KflagBuddy = 1 << 10
const KflagMmap = 1 << 11
const KflagAnon = 1 << 12
const KflagSwapcache = 1 << 13
const KflagSwapbacked = 1 << 14
const KflagCompoundHead = 1 << 15
const KflagCompoundTail = 1 << 16
const KflagHuge = 1 << 17
const KflagUnevictable = 1 << 18
const KflagHwPoison = 1 << 19
const KflagNopage = 1 << 20
const KflagKSM = 1 << 21
const KflagTHP = 1 << 22
const KflagBalloon = 1 << 23
const KflagZeroPage = 1 << 24
const KflagIdle = 1 << 25

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
	Size           int
	KernelPageSize int
	MMUPageSize    int
	Rss            int
	Pss            int
	PssDirty       int
	SharedClean    int
	SharedDirty    int
	PrivateClean   int
	PrivateDirty   int
	Referenced     int
	Anonymous      int
	KSM            int
	LazyFree       int
	AnonHugePages  int
	ShmemPmdMapped int
	FilePmdMapped  int
	SharedHugetlb  int
	PrivateHugetlb int
	Swap           int
	SwapPss        int
	Locked         int
	THPeligible    string
	ProtectionKey  string
	VMFlags        string
}

func NewProcessMemory(pid int) *ProcessMemory {
	return &ProcessMemory{pid: pid}
}

/**
 *
 *
 */
func (pm *ProcessMemory) GetMemoryRange(dev string) (uint64, uint64, error) {
	maps, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", pm.pid))
	if err != nil {
		return 0, 0, err
	}
	lines := strings.Split(string(maps), "\n")
	for _, l := range lines {
		data := strings.Fields(l)
		if len(data) == 6 && data[5] == dev {
			mems := strings.Split(data[0], "-")
			memStart, err := strconv.ParseUint(mems[0], 16, 64)
			if err != nil {
				return 0, 0, err
			}
			memEnd, err := strconv.ParseUint(mems[1], 16, 64)
			if err != nil {
				return 0, 0, err
			}
			return memStart, memEnd, nil
		}
	}
	return 0, 0, errors.New("device not found in maps")
}

/**
 * GetSmap - Get the smap data for a given mapped device.
 *
 */
func (pm *ProcessMemory) GetSmap(dev string) (*Smap, error) {
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
				switch v[0] {
				case "THPeligible:":
					smap.THPeligible = v[1]
				case "ProtectionKey:":
					smap.ProtectionKey = v[1]
				case "VmFlags:":
					smap.VMFlags = v[1]
				default:
					kvalue, kerr := strconv.ParseInt(v[1], 10, 64)
					if kerr != nil {
						return nil, kerr
					}
					switch v[0] {
					case "Size:":
						smap.Size = int(kvalue) * 1024
					case "KernelPageSize:":
						smap.KernelPageSize = int(kvalue) * 1024
					case "MMUPageSize:":
						smap.MMUPageSize = int(kvalue) * 1024
					case "Rss:":
						smap.Rss = int(kvalue) * 1024
					case "Pss:":
						smap.Pss = int(kvalue) * 1024
					case "Pss_Dirty:":
						smap.PssDirty = int(kvalue) * 1024
					case "Shared_Clean:":
						smap.SharedClean = int(kvalue) * 1024
					case "Shared_Dirty:":
						smap.SharedDirty = int(kvalue) * 1024
					case "Private_Clean:":
						smap.PrivateClean = int(kvalue) * 1024
					case "Private_Dirty:":
						smap.PrivateDirty = int(kvalue) * 1024
					case "Referenced:":
						smap.Referenced = int(kvalue) * 1024
					case "Anonymous:":
						smap.Anonymous = int(kvalue) * 1024
					case "KSM:":
						smap.KSM = int(kvalue) * 1024
					case "LazyFree:":
						smap.LazyFree = int(kvalue) * 1024
					case "AnonHugePages:":
						smap.AnonHugePages = int(kvalue) * 1024
					case "ShmemPmdMapped:":
						smap.ShmemPmdMapped = int(kvalue) * 1024
					case "FilePmdMapped:":
						smap.FilePmdMapped = int(kvalue) * 1024
					case "Shared_Hugetlb:":
						smap.SharedHugetlb = int(kvalue) * 1024
					case "Private_Hugetlb:":
						smap.PrivateHugetlb = int(kvalue) * 1024
					case "Swap:":
						smap.Swap = int(kvalue) * 1024
					case "SwapPss:":
						smap.SwapPss = int(kvalue) * 1024
					case "Locked:":
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
 * ClearSoftDirty clears the soft dirty flags
 *
 */
func (pm *ProcessMemory) ClearSoftDirty() error {
	return os.WriteFile(fmt.Sprintf("/proc/%d/clear_refs", pm.pid), []byte("4"), 0666)
}

/**
 * CopySoftDirtyMemory
 *
 */
func (pm *ProcessMemory) CopySoftDirtyMemory(addrStart uint64, addrEnd uint64, prov storage.Provider) (uint64, error) {
	bytesRead := uint64(0)
	memf, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pm.pid), os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer memf.Close()

	f, err := os.OpenFile(fmt.Sprintf("/proc/%d/pagemap", pm.pid), os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// seek, and read
	pos := int64((addrStart >> PageShift) << 3)
	_, err = f.Seek(pos, io.SeekStart)
	if err != nil {
		return 0, err
	}

	dataBuffer := make([]byte, ReadBufferSize) // Max read size

	copyData := func(start uint64, end uint64) error {
		length := end - start
		_, err := memf.ReadAt(dataBuffer[:length], int64(start))
		if err != nil {
			return err
		}
		// NB here we adjust for the start of memory
		_, err = prov.WriteAt(dataBuffer[:length], int64(start-addrStart))
		return err
	}

	currentStart := uint64(0)
	currentEnd := uint64(0)

	numPages := ((addrEnd - addrStart) + PageSize - 1) / PageSize
	pageBuffer := make([]byte, numPages<<3)
	_, err = f.Read(pageBuffer)
	if err != nil {
		return 0, err
	}

	dataIndex := 0
	for xx := addrStart; xx < addrEnd; xx += PageSize {

		val := binary.LittleEndian.Uint64(pageBuffer[dataIndex:])
		dataIndex += 8

		if (val & PagemapFlagPresent) == PagemapFlagPresent {
			if (val & PagemapFlagSoftDirty) == PagemapFlagSoftDirty {
				if currentEnd == xx {
					if currentEnd-currentStart+PageSize > uint64(len(dataBuffer)) {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return 0, err
						}
						bytesRead += (currentEnd - currentStart)

						currentStart = xx
						currentEnd = xx + PageSize
					} else {
						currentEnd = xx + PageSize
					}
				} else {
					if currentEnd != 0 {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return 0, err
						}
						bytesRead += (currentEnd - currentStart)
					}
					currentStart = xx
					currentEnd = xx + PageSize
				}
			}
		}
	}

	if currentEnd != 0 {
		err = copyData(currentStart, currentEnd)
		if err != nil {
			return 0, err
		}
		bytesRead += (currentEnd - currentStart)
	}

	return bytesRead, nil
}

/**
 * CopyMemoryRanges
 *
 */
func (pm *ProcessMemory) CopyMemoryRanges(addrStart uint64, ranges []MemoryRange, prov storage.Provider) (uint64, error) {
	bytesRead := uint64(0)
	memf, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pm.pid), os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer memf.Close()

	dataBuffer := make([]byte, ReadBufferSize) // Max read size

	copyData := func(start uint64, end uint64) error {
		length := end - start
		_, err := memf.ReadAt(dataBuffer[:length], int64(start))
		if err != nil {
			return err
		}
		// NB here we adjust for the start of memory
		_, err = prov.WriteAt(dataBuffer[:length], int64(start-addrStart))
		return err
	}

	for _, r := range ranges {
		// Chop it up into blocks if we need to, and read them sequentially.
		if r.End-r.Start > ReadBufferSize {
			for b := uint64(0); b < (r.End - r.Start); b += ReadBufferSize {
				end := r.Start + b + ReadBufferSize
				if end > r.End {
					end = r.End
				}
				err := copyData(r.Start+b, end)
				if err != nil {
					return 0, err
				}
			}
		} else {
			err := copyData(r.Start, r.End)
			if err != nil {
				return 0, err
			}
		}
		bytesRead += (r.End - r.Start)
	}

	return bytesRead, nil
}

type MemoryRange struct {
	Start uint64
	End   uint64
}

/**
 * ReadSoftDirtyMemoryRangeList
 *
 */
func (pm *ProcessMemory) ReadSoftDirtyMemoryRangeList(addrStart uint64, addrEnd uint64, lockCB func(), unlockCB func()) ([]MemoryRange, error) {
	ranges := make([]MemoryRange, 0)

	f, err := os.OpenFile(fmt.Sprintf("/proc/%d/pagemap", pm.pid), os.O_RDONLY, 0)
	if err != nil {
		return ranges, err
	}
	defer f.Close()

	// seek, and read
	pos := int64((addrStart >> PageShift) << 3)
	_, err = f.Seek(pos, io.SeekStart)
	if err != nil {
		return ranges, err
	}

	numPages := ((addrEnd - addrStart) + PageSize - 1) / PageSize
	pageBuffer := make([]byte, numPages<<3)
	lockCB()
	_, err = f.Read(pageBuffer)
	unlockCB()
	if err != nil {
		return ranges, err
	}

	currentStart := uint64(0)
	currentEnd := uint64(0)

	dataIndex := 0
	for xx := addrStart; xx < addrEnd; xx += PageSize {
		val := binary.LittleEndian.Uint64(pageBuffer[dataIndex:])
		dataIndex += 8

		if (val & PagemapFlagPresent) == PagemapFlagPresent {
			if (val & PagemapFlagSoftDirty) == PagemapFlagSoftDirty {
				if currentEnd == xx {
					currentEnd = xx + PageSize
				} else {
					if currentEnd != 0 {
						ranges = append(ranges, MemoryRange{
							Start: currentStart,
							End:   currentEnd,
						})
					}
					currentStart = xx
					currentEnd = xx + PageSize
				}
			}
		}
	}

	if currentEnd != 0 {
		ranges = append(ranges, MemoryRange{
			Start: currentStart,
			End:   currentEnd,
		})
	}

	return ranges, nil
}

/**
 * CopyDirtyMemory
 *
 */
func (pm *ProcessMemory) CopyDirtyMemory(addrStart uint64, addrEnd uint64, prov storage.Provider) error {
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
	pos := int64((addrStart >> PageShift) << 3)
	_, err = f.Seek(pos, io.SeekStart)
	if err != nil {
		return err
	}

	kdata := make([]byte, 8)
	data := make([]byte, 8)
	dataBuffer := make([]byte, ReadBufferSize) // Max read size

	copyData := func(start uint64, end uint64) error {
		length := end - start
		_, err := memf.ReadAt(dataBuffer[:length], int64(start))
		if err != nil {
			return err
		}
		// NB here we adjust for the start of memory
		_, err = prov.WriteAt(dataBuffer[:length], int64(start-addrStart))
		return err
	}

	currentStart := uint64(0)
	currentEnd := uint64(0)

	for xx := addrStart; xx < addrEnd; xx += PageSize {
		_, err = f.Read(data)
		if err != nil {
			return err
		}

		val := binary.LittleEndian.Uint64(data)
		if (val & PagemapFlagPresent) == PagemapFlagPresent {
			pfn := val & PagemapMaskPfn
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

			if (kval & KflagDirty) == KflagDirty {
				if currentEnd == xx {
					if currentEnd-currentStart+PageSize > uint64(len(dataBuffer)) {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return err
						}

						currentStart = xx
						currentEnd = xx + PageSize
					} else {
						currentEnd = xx + PageSize
					}
				} else {
					if currentEnd != 0 {
						err = copyData(currentStart, currentEnd)
						if err != nil {
							return err
						}
					}
					currentStart = xx
					currentEnd = xx + PageSize
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

const waitProcessChangeTimeout = 10 * time.Second

func (pm *ProcessMemory) PauseProcess() error {
	return pm.signalProcess(syscall.SIGSTOP, "T (stopped)")
}

func (pm *ProcessMemory) ResumeProcess() error {
	return pm.signalProcess(syscall.SIGCONT, "S (sleeping)")
}

// signalProcess send a signal
func (pm *ProcessMemory) signalProcess(sig syscall.Signal, expect string) error {

	// Send a signal to STOP
	err := syscall.Kill(pm.pid, sig)
	if err != nil {
		return err
	}

	// Wait until it's stopped
	waitTick := time.NewTicker(10 * time.Millisecond)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), waitProcessChangeTimeout)
	defer waitCancel()
waitStop:
	for {
		select {
		case <-waitCtx.Done():
			return errors.New("Could not stop process?")
		case <-waitTick.C:
			state := ""
			dd, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pm.pid))
			if err == nil {
				lines := strings.Split(string(dd), "\n")
				for _, l := range lines {
					if strings.HasPrefix(l, "State:") {
						state = strings.Trim(l[6:], "\r\n \t")
						if state == expect {
							break waitStop
						}
					}
				}
			}
		}
	}
	return nil
}
