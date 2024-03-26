package modules

import (
	"crypto/rand"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestCopyOnWriteReads(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	srcMem := sources.NewMemoryStorage(size)
	mem := NewMetrics(srcMem)
	srcCache := sources.NewMemoryStorage(size)
	cache := NewMetrics(srcCache)

	// Fill it with stuff
	data := make([]byte, size)
	rand.Read(data)
	_, err := srcMem.WriteAt(data, 0)
	assert.NoError(t, err)

	cow := NewCopyOnWrite(mem, cache, 10)

	// Now try doing some reads...

	buff := make([]byte, 30)
	_, err = cow.ReadAt(buff, 18)
	assert.NoError(t, err)

	// Check the data is correct...
	assert.Equal(t, data[18:48], buff)

	n := cow.exists.Count(0, cow.exists.Length())
	assert.Equal(t, 4, n)

	snapMem := mem.Snapshot()
	snapCache := cache.Snapshot()

	assert.Equal(t, uint64(4), snapMem.Read_ops)
	assert.Equal(t, uint64(0), snapMem.Write_ops)

	assert.Equal(t, uint64(0), snapCache.Read_ops)
	assert.Equal(t, uint64(4), snapCache.Write_ops)

	// Do another read that spans past this one...

	buff2 := make([]byte, 50)
	_, err = cow.ReadAt(buff2, 8)
	assert.NoError(t, err)

	assert.Equal(t, data[8:58], buff2)

	// There should have been TWO more reads from source, and some from cache
	n2 := cow.exists.Count(0, cow.exists.Length())
	assert.Equal(t, 6, n2)

	snapMem2 := mem.Snapshot()
	snapCache2 := cache.Snapshot()

	assert.Equal(t, uint64(6), snapMem2.Read_ops)
	assert.Equal(t, uint64(0), snapMem2.Write_ops)

	assert.Equal(t, uint64(4), snapCache2.Read_ops)
	assert.Equal(t, uint64(6), snapCache2.Write_ops)

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
	rand.Read(data)
	_, err := srcMem.WriteAt(data, 0)
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

	buff3 := make([]byte, 100)
	// Read from src
	copy(buff3, data)
	// Merge in write
	copy(buff3[18:], buff)

	assert.Equal(t, buff3, buff2)
}
