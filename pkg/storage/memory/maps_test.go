package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaps(t *testing.T) {
	myPID := os.Getpid()

	map1, err := GetMaps(myPID)
	assert.NoError(t, err)

	totalMemory := uint64(0)
	totalRO := uint64(0)
	for _, v := range map1.Entries {
		totalMemory += (v.AddrEnd - v.AddrStart)
		if v.PermRead && !v.PermWrite {
			totalRO += (v.AddrEnd - v.AddrStart)
		}
	}

	fmt.Printf("Total memory %d, Read Only %d\n", totalMemory, totalRO)

	// mmap a file, and make sure it shows up

	file, err := filepath.Abs("testdata")
	assert.NoError(t, err)

	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	assert.NoError(t, err)
	fileinfo, err := f.Stat()
	assert.NoError(t, err)

	offset := 4096
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	mmdata, err := syscall.Mmap(int(f.Fd()), int64(offset), int(fileinfo.Size())-offset, prot, syscall.MAP_SHARED)
	assert.NoError(t, err)

	// Perform cleanup.
	t.Cleanup(func() {
		err = syscall.Munmap(mmdata)
		assert.NoError(t, err)

		err = f.Close()
		assert.NoError(t, err)
	})

	// Check the maps again here...

	map2, err := GetMaps(myPID)
	assert.NoError(t, err)

	// Make sure maps is right...

	totalMemory2 := uint64(0)
	totalRO2 := uint64(0)
	for l, v := range map2.Entries {
		fmt.Printf("%d | %v\n", l, v)
		totalMemory2 += (v.AddrEnd - v.AddrStart)
		if v.PermRead && !v.PermWrite {
			totalRO2 += (v.AddrEnd - v.AddrStart)
		}
	}

	fmt.Printf("Total memory2 %d, Read Only %d\n", totalMemory2, totalRO2)

	// There should be more memory mapped to the process
	assert.Greater(t, totalMemory2, totalMemory)

	// Look for the entry
	matches := map2.FindPathname(file)

	assert.Equal(t, 1, len(matches))

	entry := matches[0]
	// Check permissions
	assert.True(t, entry.PermRead)
	assert.True(t, entry.PermWrite)
	assert.True(t, entry.PermShared)

	assert.Equal(t, offset, int(entry.Offset))

	assert.Equal(t, int(fileinfo.Size())-offset, int(entry.AddrEnd-entry.AddrStart))

	for _, e := range matches {
		fmt.Printf("- %v\n", e)
	}
}
