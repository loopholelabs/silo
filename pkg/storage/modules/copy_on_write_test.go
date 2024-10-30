package modules

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestCopyOnWriteReads(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	mem := sources.NewMemoryStorage(size)
	cache := sources.NewMemoryStorage(size)

	// Fill it with stuff
	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	_, err = mem.WriteAt(data, 0)
	assert.NoError(t, err)

	cow := NewCopyOnWrite(mem, cache, 10)

	// Now try doing some reads...

	buff := make([]byte, 30)
	_, err = cow.ReadAt(buff, 18)
	assert.NoError(t, err)

	// Check the data is correct...
	assert.Equal(t, data[18:48], buff)

	n := cow.exists.Count(0, cow.exists.Length())
	assert.Equal(t, 0, n)

	// Do another read that spans past this one...

	buff2 := make([]byte, 50)
	_, err = cow.ReadAt(buff2, 8)
	assert.NoError(t, err)

	assert.Equal(t, data[8:58], buff2)

	n2 := cow.exists.Count(0, cow.exists.Length())
	assert.Equal(t, 0, n2)

}

func TestCopyOnWriteWrites(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	srcMem := sources.NewMemoryStorage(size)
	mem := NewMetrics(srcMem)
	srcCache := sources.NewMemoryStorage(size)
	cache := NewMetrics(srcCache)

	// Fill it with stuff
	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	_, err = srcMem.WriteAt(data, 0)
	assert.NoError(t, err)

	cow := NewCopyOnWrite(mem, cache, 10)

	// Now try doing some writes...

	buff := make([]byte, 30)
	_, err = cow.WriteAt(buff, 18)
	assert.NoError(t, err)

	n := cow.exists.Count(0, cow.exists.Length())
	assert.Equal(t, 4, n)

	// Read some back make sure it looks ok...

	buff2 := make([]byte, 100)
	_, err = cow.ReadAt(buff2, 0)
	assert.NoError(t, err)

	buff3 := make([]byte, 100)
	// Read from src
	copy(buff3, data)
	// Merge in write
	copy(buff3[18:], buff)

	assert.Equal(t, buff3, buff2)
}

func TestCopyOnWriteReadOverrun(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	srcMem := sources.NewMemoryStorage(size)
	mem := NewMetrics(srcMem)
	srcCache := sources.NewMemoryStorage(size)
	cache := NewMetrics(srcCache)

	// Fill it with stuff
	data := make([]byte, size+10) // Some extra
	_, err := rand.Read(data)
	assert.NoError(t, err)
	onlydata := data[:size]
	n, err := srcMem.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	cow := NewCopyOnWrite(mem, cache, 1024)

	buff2 := make([]byte, 100)
	n, err = cow.ReadAt(buff2, int64(size-50))

	assert.NoError(t, err)
	assert.Equal(t, 50, n)

	assert.Equal(t, onlydata[size-50:], buff2[:50])
}

func TestCopyOnWriteReadOverrunNonMultiple(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	srcMem := sources.NewMemoryStorage(size)
	mem := NewMetrics(srcMem)
	srcCache := sources.NewMemoryStorage(size)
	cache := NewMetrics(srcCache)

	// Fill it with stuff
	data := make([]byte, size+10)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	onlydata := data[:size]

	cow := NewCopyOnWrite(mem, cache, 1000) // NB 1024*1024 isn't multiple of 1000 blocksize

	n, err := cow.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	buff2 := make([]byte, 100)
	n, err = cow.ReadAt(buff2, int64(size-50))

	assert.NoError(t, err)
	assert.Equal(t, 50, n)

	assert.Equal(t, onlydata[size-50:], buff2[:50])
}

func TestCopyOnWriteCRCIssue(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 16 * 1024
	blockSize := 64 * 1024

	fstore, err := sources.NewFileStorageSparseCreate("test_data_sparse", uint64(size), blockSize)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	rosource := sources.NewMemoryStorage(size)

	fstoreLog := NewLogger(fstore, "fstore")

	cow := NewCopyOnWrite(rosource, fstoreLog, blockSize)

	reference := sources.NewMemoryStorage(size)

	// Fill it with random stuff
	data := make([]byte, size)
	_, err = rand.Read(data)
	assert.NoError(t, err)
	_, err = rosource.WriteAt(data, 0)
	assert.NoError(t, err)
	_, err = reference.WriteAt(data, 0)
	assert.NoError(t, err)

	// Now do some WriteAt() calls, and then a big ReadAt() and make sure they match

	doWrite := func(offset int64) {
		buffer := make([]byte, 4096)
		_, err := rand.Read(buffer)
		assert.NoError(t, err)

		n, err := cow.WriteAt(buffer, offset)
		assert.NoError(t, err)
		assert.Equal(t, n, 4096)

		n, err = reference.WriteAt(buffer, offset)
		assert.NoError(t, err)
		assert.Equal(t, n, 4096)
	}

	doWrite(8192)
	doWrite(0)
	doWrite(4096)

	// Now check they are equal

	eq, err := storage.Equals(reference, fstore, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	eq, err = storage.Equals(reference, cow, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	buff_cow := make([]byte, cow.Size())
	_, err = cow.ReadAt(buff_cow, 0)
	assert.NoError(t, err)

	buff_ref := make([]byte, reference.Size())
	_, err = reference.ReadAt(buff_ref, 0)
	assert.NoError(t, err)
}
