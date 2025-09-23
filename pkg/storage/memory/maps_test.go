package memory

import (
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

	// mmap a file, and make sure it shows up

	file, err := filepath.Abs("testdata")
	assert.NoError(t, err)

	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	assert.NoError(t, err)
	fileinfo, err := f.Stat()
	assert.NoError(t, err)

	offset := 4096
	mapsize := int(fileinfo.Size()) - offset
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	mmdata, err := syscall.Mmap(int(f.Fd()), int64(offset), mapsize, prot, syscall.MAP_SHARED)
	assert.NoError(t, err)

	// Check the maps again here...

	map2, err := GetMaps(myPID)
	assert.NoError(t, err)

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
	assert.Equal(t, mapsize, int(entry.AddrEnd-entry.AddrStart))

	// Check we can also lookup by the address range
	matches2 := map2.FindMemoryRange(entry.AddrStart, entry.AddrEnd)
	assert.Equal(t, 1, len(matches2))
	assert.True(t, entry.Equal(matches2[0]))

	// Check we can also lookup by an address inside
	matches3 := map2.FindAddressPage(entry.AddrStart + PageSize)
	assert.Equal(t, 1, len(matches3))
	assert.True(t, entry.Equal(matches3[0]))

	// Look at the diffs in the maps
	newRanges1 := map2.Sub(map1)

	// We expect there to be a new one
	assert.Greater(t, len(newRanges1.Entries), 0)

	newpages := map1.AddedPages(map2)
	assert.GreaterOrEqual(t, len(newpages), (mapsize+PageSize-1)/PageSize)

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
