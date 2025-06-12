package memory

import (
	"crypto/rand"
	"os"
	"path"
	"syscall"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestMemoryProvider(t *testing.T) {
	file1 := "test_file"

	prov, err := sources.NewFileStorageCreate(file1, 8*1024*1024)
	assert.NoError(t, err)

	// mmap the device as well...
	f, err := os.OpenFile(file1, os.O_RDWR, 0666)
	assert.NoError(t, err)

	// mmap the device in a few chunks
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	datas := make([][]byte, 0)

	chunkSize := 1024 * 1024
	for offset := 0; offset < int(prov.Size()); offset += chunkSize {
		mmdata, err := syscall.Mmap(int(f.Fd()), int64(offset), chunkSize, prot, syscall.MAP_SHARED)
		assert.NoError(t, err)
		datas = append(datas, mmdata)
	}

	// Perform cleanup.
	t.Cleanup(func() {
		for _, m := range datas {
			err = syscall.Munmap(m)
			assert.NoError(t, err)
		}

		err = f.Close()
		assert.NoError(t, err)

		os.Remove(file1)
	})

	// Now try doing some things...
	wd, err := os.Getwd()
	assert.NoError(t, err)
	mem, err := NewProcessMemoryStorage(os.Getpid(), path.Join(wd, file1))
	assert.NoError(t, err)

	buffer := make([]byte, prov.Size())
	_, err = rand.Read(buffer)
	assert.NoError(t, err)
	nbytes, err := prov.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), nbytes)

	// Check they're equal
	eq, err := storage.Equals(prov, mem, 1024*1024)
	assert.NoError(t, err)
	assert.True(t, eq)

	_, err = rand.Read(buffer)
	assert.NoError(t, err)
	nbytes, err = mem.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), nbytes)

	// Check they're equal
	eq, err = storage.Equals(prov, mem, 1024*1024)
	assert.NoError(t, err)
	assert.True(t, eq)

}
