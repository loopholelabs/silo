package memory

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaps(t *testing.T) {
	myPID := os.Getpid()

	maps, err := GetMaps(myPID)
	assert.NoError(t, err)

	totalMemory := uint64(0)
	totalRO := uint64(0)
	for _, v := range maps {
		totalMemory += (v.AddrEnd - v.AddrStart)
		if v.PermRead && !v.PermWrite {
			totalRO += (v.AddrEnd - v.AddrStart)
		}
	}

	fmt.Printf("Total memory %d, Read Only %d\n", totalMemory, totalRO)

	// mmap a file, and make sure it shows up

	f, err := os.OpenFile("testdata", os.O_RDWR, 0666)
	assert.NoError(t, err)
	fileinfo, err := f.Stat()
	assert.NoError(t, err)

	prot := syscall.PROT_READ | syscall.PROT_WRITE
	mmdata, err := syscall.Mmap(int(f.Fd()), 0, int(fileinfo.Size()), prot, syscall.MAP_SHARED)
	assert.NoError(t, err)

	// Perform cleanup.
	t.Cleanup(func() {
		err = syscall.Munmap(mmdata)
		assert.NoError(t, err)

		err = f.Close()
		assert.NoError(t, err)
	})

	// Check the maps again here...

	maps2, err := GetMaps(myPID)
	assert.NoError(t, err)

	// Make sure maps is right...

	totalMemory2 := uint64(0)
	totalRO2 := uint64(0)
	for l, v := range maps2 {
		fmt.Printf("%d | %v\n", l, v)
		totalMemory2 += (v.AddrEnd - v.AddrStart)
		if v.PermRead && !v.PermWrite {
			totalRO2 += (v.AddrEnd - v.AddrStart)
		}
	}

	fmt.Printf("Total memory2 %d, Read Only %d\n", totalMemory2, totalRO2)

	// There should be more memory mapped to the process
	assert.Greater(t, totalMemory2, totalMemory)
}
