package memory

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const MapsPermRead = 'r'
const MapsPermWrite = 'w'
const MapsPermExecute = 'x'
const MapsPermShared = 's'
const MapsPermPrivate = 'p'
const MapsPermUnknown = '?'

type MapsFile struct {
	Pid     int
	Entries []*MapsEntry
}

type MapsEntry struct {
	AddrStart   uint64
	AddrEnd     uint64
	PermRead    bool
	PermWrite   bool
	PermExecute bool
	PermShared  bool
	PermPrivate bool
	Offset      uint64
	Dev         string
	Inode       string
	Pathname    string
}

func (me *MapsEntry) Equal(me2 *MapsEntry) bool {
	return me.AddrStart == me2.AddrStart &&
		me.AddrEnd == me2.AddrEnd &&
		me.PermRead == me2.PermRead &&
		me.PermWrite == me2.PermWrite &&
		me.PermExecute == me2.PermExecute &&
		me.PermShared == me2.PermShared &&
		me.PermPrivate == me2.PermPrivate &&
		me.Offset == me2.Offset &&
		me.Dev == me2.Dev &&
		me.Inode == me2.Inode &&
		me.Pathname == me2.Pathname
}

func (me *MapsEntry) String() string {
	perms := []byte("----")
	if me.PermRead {
		perms[0] = MapsPermRead
	}
	if me.PermWrite {
		perms[1] = MapsPermWrite
	}
	if me.PermExecute {
		perms[2] = MapsPermExecute
	}
	switch {
	case me.PermPrivate && !me.PermShared:
		perms[3] = MapsPermPrivate
	case me.PermShared && !me.PermPrivate:
		perms[3] = MapsPermShared
	default:
		perms[3] = MapsPermUnknown
	}

	return fmt.Sprintf("%016x-%016x %s %08x %s %s %s", me.AddrStart, me.AddrEnd, perms, me.Offset, me.Dev, me.Inode, me.Pathname)
}

// GetMaps reads the /proc/<PID>/maps file and parses.
func GetMaps(pid int) (*MapsFile, error) {
	maps, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", pid))
	if err != nil {
		return nil, err
	}

	entries := make([]*MapsEntry, 0)

	// Look through for the data we need...
	lines := strings.Split(string(maps), "\n")
	for _, line := range lines {
		// Read the header...
		headerData := strings.Fields(line)
		// eg
		// 7b0bcb38c000-7b0bcb39e000 rw-p 00279000 07:00 8261                       /snap/code/206/usr/share/code/libffmpeg.so
		entry := &MapsEntry{}

		// Parse address
		if len(headerData) > 0 {
			addrBits := strings.Split(headerData[0], "-")
			if len(addrBits) == 2 {
				addrStart, err := strconv.ParseUint(addrBits[0], 16, 64)
				if err != nil {
					return nil, err
				}
				entry.AddrStart = addrStart
				addrEnd, err := strconv.ParseUint(addrBits[1], 16, 64)
				if err != nil {
					return nil, err
				}
				entry.AddrEnd = addrEnd
			}

			// Perms
			if len(headerData) > 1 {
				for _, c := range headerData[1] {
					switch c {
					case MapsPermRead:
						entry.PermRead = true
					case MapsPermWrite:
						entry.PermWrite = true
					case MapsPermExecute:
						entry.PermExecute = true
					case MapsPermPrivate:
						entry.PermPrivate = true
					case MapsPermShared:
						entry.PermShared = true
					}
				}
			}
			// Offset
			if len(headerData) > 2 {
				offset, err := strconv.ParseUint(headerData[2], 16, 64)
				if err != nil {
					return nil, err
				}
				entry.Offset = offset
			}
			// Dev
			if len(headerData) > 3 {
				entry.Dev = headerData[3]
			}

			// Inode
			if len(headerData) > 4 {
				entry.Inode = headerData[4]
			}

			// Pathname
			if len(headerData) > 5 {
				entry.Pathname = headerData[5]
			}

			entries = append(entries, entry)
		}
	}

	return &MapsFile{
		Pid:     pid,
		Entries: entries,
	}, nil
}

// FindPathname searches for all matches for the given pathname (exact match)
func (mf *MapsFile) FindPathname(pathname string) []*MapsEntry {
	matches := make([]*MapsEntry, 0)
	for _, e := range mf.Entries {
		if e.Pathname == pathname {
			matches = append(matches, e)
		}
	}
	return matches
}

// FindMemoryRange searches for all matches for the given memory range (exact match)
func (mf *MapsFile) FindMemoryRange(addrStart uint64, addrEnd uint64) []*MapsEntry {
	matches := make([]*MapsEntry, 0)
	for _, e := range mf.Entries {
		if e.AddrStart == addrStart && e.AddrEnd == addrEnd {
			matches = append(matches, e)
		}
	}
	return matches
}

// FindAddress searches for all matches for the given memory address page (contains)
func (mf *MapsFile) FindAddressPage(addr uint64) []*MapsEntry {
	matches := make([]*MapsEntry, 0)
	for _, e := range mf.Entries {
		if addr >= e.AddrStart && addr+PageSize <= e.AddrEnd {
			matches = append(matches, e)
		}
	}
	return matches
}

// Sub returns things in mf which aren't *exactly* in mf2
func (mf *MapsFile) Sub(mf2 *MapsFile) *MapsFile {
	entries := make([]*MapsEntry, 0)
	for _, v := range mf.Entries {
		matches := mf2.FindMemoryRange(v.AddrStart, v.AddrEnd)
		if len(matches) != 1 || !matches[0].Equal(v) {
			entries = append(entries, v)
		}
	}
	return &MapsFile{
		Pid:     mf.Pid,
		Entries: entries,
	}
}

// Total memory size
func (mf *MapsFile) Size() uint64 {
	total := uint64(0)
	for _, v := range mf.Entries {
		total += v.AddrEnd - v.AddrStart
	}
	return total
}

// AddedPages works out which memory pages have been added from mf->mf2
func (mf *MapsFile) AddedPages(mf2 *MapsFile) []uint64 {
	pages := make([]uint64, 0)
	diff := mf2.Sub(mf) // Only look at entries in mf2 that differ

	// NB: This is not optimized, but we can revisit later if needed.
	for _, e := range diff.Entries {
		for a := e.AddrStart; a < e.AddrEnd; a += PageSize {
			matches := mf.FindAddressPage(a)
			if len(matches) == 0 {
				// It's been added
				pages = append(pages, a)
			}
		}
	}
	return pages
}
