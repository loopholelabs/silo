package modules

import (
	"crypto/rand"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestCopyOnWriteMulti(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	mem := sources.NewMemoryStorage(size)

	// Fill the base with known data
	data := make([]byte, size)
	for i:=0;i<len(data);i++ {
		data[i] = 99
	}
	_, err := mem.WriteAt(data, 0)
	assert.NoError(t, err)

	cache1 := sources.NewMemoryStorage(size)
	cow1 := NewCopyOnWrite(mem, cache1, 10, true, nil)

	cache2 := sources.NewMemoryStorage(size)
	cow2 := NewCopyOnWrite(cow1, cache2, 10, true, nil)

	cache3 := sources.NewMemoryStorage(size)
	cow3 := NewCopyOnWrite(cow2, cache3, 10, true, nil)

	// First check that reads all work and look as we expect at each level.
	for _, c := range []storage.Provider{cow1, cow2, cow3} {
		buff1 := make([]byte, size)
		_, err = c.ReadAt(buff1, 0)
		assert.NoError(t, err)
		assert.Equal(t, buff1, data)
	}

	// Now do some writes to each layer (non overlapping)

	dataWrite1 := []byte{0, 0, 0, 0, 0, 0}
	_, err = cow1.WriteAt(dataWrite1, 0)
	assert.NoError(t, err)
	dataWrite2 := []byte{1, 1, 1, 1, 1, 1}
	_, err = cow2.WriteAt(dataWrite2, 100)
	assert.NoError(t, err)
	dataWrite3 := []byte{2, 2, 2, 2, 2, 2}
	_, err = cow3.WriteAt(dataWrite3, 200)
	assert.NoError(t, err)

	// Check reads are as expected...
	for _, c := range []storage.Provider{cow1, cow2, cow3} {
		// First lot should appear in all layers.
		buff1 := make([]byte, 6)
		_, err = c.ReadAt(buff1, 0)
		assert.NoError(t, err)
		assert.Equal(t, buff1, dataWrite1)

		// Second lot should appear in cow2 and cow3 but NOT in cow1
		buff2 := make([]byte, 6)
		_, err = c.ReadAt(buff2, 100)
		assert.NoError(t, err)
		if c == cow1 {
			assert.NotEqual(t, buff2, dataWrite2)
		} else {
			assert.Equal(t, buff2, dataWrite2)
		}

		// thired lot should appear in cow3 but NOT in cow1 or cow2
		buff3 := make([]byte, 6)
		_, err = c.ReadAt(buff3, 200)
		assert.NoError(t, err)
		if c == cow1 || c == cow2 {
			assert.NotEqual(t, buff3, dataWrite3)
		} else {
			assert.Equal(t, buff3, dataWrite3)
		}
	}
}

func TestCopyOnWriteMultiOverlap(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 100
	mem := sources.NewMemoryStorage(size)

	// Fill the base with known data
	data := make([]byte, size)
	for i:=0;i<len(data);i++ {
		data[i] = 99
	}
	_, err := mem.WriteAt(data, 0)
	assert.NoError(t, err)

	cache1 := sources.NewMemoryStorage(size)
	cow1 := NewCopyOnWrite(mem, cache1, 10, true, nil)

	cache2 := sources.NewMemoryStorage(size)
	cow2 := NewCopyOnWrite(cow1, cache2, 10, true, nil)

	cache3 := sources.NewMemoryStorage(size)
	cow3 := NewCopyOnWrite(cow2, cache3, 10, true, nil)

	dataWrite1 := []byte{0, 0, 0, 0, 0, 0}
	_, err = cow1.WriteAt(dataWrite1, 0)
	assert.NoError(t, err)
	dataWrite2 := []byte{1, 1, 1, 1, 1, 1}
	_, err = cow2.WriteAt(dataWrite2, 5)
	assert.NoError(t, err)
	dataWrite3 := []byte{2, 2, 2, 2, 2, 2}
	_, err = cow3.WriteAt(dataWrite3, 7)
	assert.NoError(t, err)

	// Do some later writes in each layer
	_, err = cow1.WriteAt(dataWrite1, 60)
	assert.NoError(t, err)
	_, err = cow2.WriteAt(dataWrite2, 70)
	assert.NoError(t, err)
	_, err = cow3.WriteAt(dataWrite3, 80)
	assert.NoError(t, err)

	// Overlap these ones
	_, err = cow1.WriteAt(dataWrite1, 90)
	assert.NoError(t, err)
	_, err = cow2.WriteAt(dataWrite2, 90)
	assert.NoError(t, err)
	_, err = cow3.WriteAt(dataWrite3, 90)
	assert.NoError(t, err)

	// Check reads are as expected from each layer...
	for _, c := range []*CopyOnWrite{cow1, cow2, cow3} {
		buff1 := make([]byte, 13) // 7 + 6
		_, err = c.ReadAt(buff1, 0)
		assert.NoError(t, err)
		switch c {
		case cow1:
			// We'd expect the first 6 to be 0s
			assert.Equal(t, dataWrite1, buff1[:6])
		case cow2:
			assert.Equal(t, []byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1}, buff1[:11])
		case cow3:
			assert.Equal(t, []byte{0, 0, 0, 0, 0, 1, 1, 2, 2, 2, 2, 2, 2}, buff1)
		}

		// Check the view in terms of unrequired blocks
		chgBlocks, chgBytes, err := c.GetDifference()
		assert.NoError(t, err)

		blocks := storage.SendSiloEvent(c, storage.EventTypeCowGetBlocks, nil)
		assert.Equal(t, 1, len(blocks))
		bl := blocks[0].([]uint)
		switch c {
		case cow1:
			assert.Equal(t, int64(3), chgBlocks)
			assert.Equal(t, int64(18), chgBytes)
			assert.Equal(t, []uint{0x1, 0x2, 0x3, 0x4, 0x5, 0x7, 0x8}, bl)
		case cow2:
			assert.Equal(t, int64(4), chgBlocks)
			assert.Equal(t, int64(18), chgBytes)
			assert.Equal(t, []uint{0x2, 0x3, 0x4, 0x5, 0x8}, bl)
		case cow3:
			assert.Equal(t, int64(4), chgBlocks)
			assert.Equal(t, int64(18), chgBytes)
			assert.Equal(t, []uint{0x2, 0x3, 0x4, 0x5}, bl)
		}
	}
}
