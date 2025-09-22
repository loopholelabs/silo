package memory

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

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
	if me.PermPrivate && !me.PermShared {
		perms[3] = MapsPermPrivate
	} else if me.PermShared && !me.PermPrivate {
		perms[3] = MapsPermShared
	} else {
		perms[3] = MapsPermUnknown
	}
	return fmt.Sprintf("%016x-%016x %s %08x %s %s %s", me.AddrStart, me.AddrEnd, perms, me.Offset, me.Dev, me.Inode, me.Pathname)
}

const MapsPermRead = 'r'
const MapsPermWrite = 'w'
const MapsPermExecute = 'x'
const MapsPermShared = 's'
const MapsPermPrivate = 'p'
const MapsPermUnknown = '?'

// GetMaps reads the /proc/<PID>/maps file and parses.
func GetMaps(pid int) ([]*MapsEntry, error) {
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

	return entries, nil
}
