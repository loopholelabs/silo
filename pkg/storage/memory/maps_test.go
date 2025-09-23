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

	fmt.Printf("Total memory %d\n", map1.Size())

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

	// Check the maps again here...

	map2, err := GetMaps(myPID)
	assert.NoError(t, err)

	fmt.Printf("Total memory2 %d\n", map2.Size())

	// There should be more memory mapped to the process
	assert.Greater(t, map2.Size(), map1.Size())

	// Look for the entry by pathname
	matches := map2.FindPathname(file)

	assert.Equal(t, 1, len(matches))

	entry := matches[0]
	// Check permissions, offset, size
	assert.True(t, entry.PermRead)
	assert.True(t, entry.PermWrite)
	assert.True(t, entry.PermShared)
	assert.Equal(t, offset, int(entry.Offset))
	assert.Equal(t, int(fileinfo.Size())-offset, int(entry.AddrEnd-entry.AddrStart))

	// Look at the diffs in the maps
	newRanges1 := map2.Sub(map1)

	// We expect there to be a new one
	assert.Greater(t, len(newRanges1.Entries), 0)

	newpages := map1.AddedPages(map2)
	oldpages := map2.AddedPages(map1)
	fmt.Printf("New pages %d Old pages %d\n", len(newpages), len(oldpages))

	// Now unmap the file
	err = syscall.Munmap(mmdata)
	assert.NoError(t, err)

	err = f.Close()
	assert.NoError(t, err)

	map3, err := GetMaps(myPID)
	assert.NoError(t, err)

	// Look at the diffs again in the maps
	oldRanges2 := map2.Sub(map3)

	// We expect at least one range to have been removed
	assert.Greater(t, len(oldRanges2.Entries), 0)

}
