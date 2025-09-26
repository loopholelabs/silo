package memory

import (
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testfile = "testdata"

func TestTracker(t *testing.T) {
	myPID := os.Getpid()

	// Create a new tracker for the processes memory
	lock := func() error {
		fmt.Printf("Lock process\n")
		return nil
	}
	unlock := func() error {
		fmt.Printf("Unlock process\n")
		return nil
	}

	tracker := NewMemoryTracker(myPID, lock, unlock)

	size := 1024 * 1024
	buffer := make([]byte, size)
	_, err := crand.Read(buffer)
	assert.NoError(t, err)
	err = os.WriteFile(testfile, buffer, 0666)
	assert.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(testfile)
	})

	// mmap a file, and make sure it shows up
	file, err := filepath.Abs(testfile)
	assert.NoError(t, err)

	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	assert.NoError(t, err)

	offset := 0
	mapsize := size - offset

	var mappedEntry *MapsEntry
	var mappedLock sync.Mutex
	mappedData := make([]byte, mapsize)
	mappedWrites := 0

	totalMemoryAdded := uint64(0)
	totalMemoryRemoved := uint64(0)
	totalMemoryModified := uint64(0)

	cb := &Callbacks{
		AddPages: func(pid int, addr []uint64) {
			fmt.Printf("# AddPages %d %d\n", pid, len(addr))
			totalMemoryAdded += uint64(len(addr) * PageSize)
		},
		RemovePages: func(pid int, addr []uint64) {
			fmt.Printf("# RemovePages %d %d\n", pid, len(addr))
			totalMemoryRemoved += uint64(len(addr) * PageSize)
		},
		UpdatePages: func(pid int, data []byte, addr uint64) error {
			totalMemoryModified += uint64(len(data))

			// Here's we'll just concentrate on the mmapped file for now...
			mappedLock.Lock()
			defer mappedLock.Unlock()

			if mappedEntry == nil {
				// Find if it's mapped yet...
				maps, err := GetMaps(myPID)
				assert.NoError(t, err)

				matches := maps.FindPathname(file)
				if len(matches) == 1 {
					mappedEntry = matches[0]
				}
			}

			// Now we can process it
			if mappedEntry != nil {
				// Update the data here...
				// Starts inside the mmap
				if addr >= mappedEntry.AddrStart && addr < mappedEntry.AddrEnd {
					n := copy(mappedData[addr-mappedEntry.AddrStart:], data)
					mappedWrites += n
					// Starts before the map, but ends either inside the map or to the right
				} else if addr < mappedEntry.AddrStart && addr+uint64(len(data)) > mappedEntry.AddrStart {
					n := copy(mappedData, data[mappedEntry.AddrStart-addr:])
					mappedWrites += n
				}
			}
			return nil
		},
	}

	prot := syscall.PROT_READ | syscall.PROT_WRITE
	mmdata, err := syscall.Mmap(int(f.Fd()), int64(offset), mapsize, prot, syscall.MAP_SHARED)
	assert.NoError(t, err)

	// Do an initial update from the memory
	_ = tracker.Update(cb)

	// NOW Change some things in the mmapped file
	for i := 0; i < 100; i++ {
		offset := rand.IntN(len(mmdata))
		mmdata[offset]++
	}

	// This should get any changes in the memory
	_ = tracker.Update(cb)

	// Make sure it picked these memory changes up
	assert.Equal(t, mmdata, mappedData)

	// Now unmap the file
	err = syscall.Munmap(mmdata)
	assert.NoError(t, err)

	err = f.Close()
	assert.NoError(t, err)

	// This should get any changes in the memory
	_ = tracker.Update(cb)

	fmt.Printf("Memory added %d bytes, removed %d bytes, modified %d bytes\n", totalMemoryAdded, totalMemoryRemoved, totalMemoryModified)
}
