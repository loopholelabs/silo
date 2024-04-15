package modules

import (
	"crypto/rand"
	mrand "math/rand"
	"os"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestRaid(t *testing.T) {
	size := 1024 * 1000
	block_size := 4096

	source, err := sources.NewFileStorageCreate("testraid_source", int64(size))
	assert.NoError(t, err)

	cache, err := sources.NewFileStorageSparseCreate("testraid_cache", uint64(size), block_size)
	//cache, err := sources.NewFileStorageCreate("testraid_cache", int64(size))
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("testraid_source")
		os.Remove("testraid_cache")
	})

	cow := NewCopyOnWrite(source, cache, block_size)

	mem := sources.NewMemoryStorage(size)

	// Setup raid devices, and make sure they all agree.
	raid, err := NewRaid([]storage.StorageProvider{cow, mem})
	assert.NoError(t, err)

	var wg sync.WaitGroup

	// Now do some concurrent writes here... Make sure they're not overlapping though, otherwise there will be execution order issues
	offset := 0
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(o int) {
			buff := make([]byte, 100)
			rand.Read(buff)
			//offset := mrand.Intn(size)
			_, err := raid.WriteAt(buff, int64(o))
			assert.NoError(t, err)
			wg.Done()
		}(offset)

		offset += 100
		if offset >= size {
			offset = 0
		}
	}
	wg.Wait()

	// Wait for them all to complete...

	equal, err := storage.Equals(cow, mem, block_size)
	assert.NoError(t, err)
	assert.Equal(t, true, equal)

	master_data := make([]byte, size)
	n, err := mem.ReadAt(master_data, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	for i := 0; i < 10000; i++ {
		// Now do concurrent reads... (The data integrity check will happen within Raid)
		wg.Add(1)
		go func() {
			buff := make([]byte, 100)
			offset := mrand.Intn(size)
			n, err := raid.ReadAt(buff, int64(offset))
			assert.NoError(t, err)
			assert.Equal(t, buff[:n], master_data[offset:offset+n])
			wg.Done()
		}()
	}

	wg.Wait()

	// Now check with an existing sparse file / cow...
	/*
		cache.Close()

		// Load existing cache up
		cache2, err := sources.NewFileStorageSparse("testraid_cache", uint64(size), block_size)
		//cache, err := sources.NewFileStorage("testraid_cache", int64(size))
		assert.NoError(t, err)

		cow2 := NewCopyOnWrite(source, cache2, block_size)
		cow2.SetBlockExists(cow.GetBlockExists())

		// Check for any corruption

		equal, err = storage.Equals(cow2, mem, block_size)
		assert.NoError(t, err)
		assert.Equal(t, true, equal)
	*/
}
