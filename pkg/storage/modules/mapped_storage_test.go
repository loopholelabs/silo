package modules

import (
	"crypto/rand"
	"slices"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestMappedStorage(t *testing.T) {
	block_size := 4096
	store := sources.NewMemoryStorage(64 * block_size)

	ms := NewMappedStorage(store, block_size)
	// Write some blocks, then read them back

	data := make([]byte, block_size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	err = ms.WriteBlock(0x12345678, data)
	assert.NoError(t, err)

	buffer := make([]byte, block_size)

	err = ms.ReadBlock(0x12345678, buffer)
	assert.NoError(t, err)

	assert.Equal(t, buffer, data)

	err = ms.ReadBlock(0xdead, buffer)
	assert.ErrorIs(t, err, Err_not_found)

	assert.Equal(t, uint64(block_size), ms.Size())

}

func TestMappedStorageRemove(t *testing.T) {
	block_size := 4096
	store := sources.NewMemoryStorage(64 * block_size)

	ms := NewMappedStorage(store, block_size)
	// Write some blocks, then read them back

	data := make([]byte, block_size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	id := uint64(0x12345678)
	err = ms.WriteBlock(id, data)
	assert.NoError(t, err)

	assert.Equal(t, uint64(block_size), ms.Size())

	err = ms.RemoveBlock(id)
	assert.NoError(t, err)

	assert.Equal(t, uint64(0), ms.Size())
}

func TestMappedStorageOutOfSpace(t *testing.T) {
	block_size := 4096
	store := sources.NewMemoryStorage(2 * block_size)

	ms := NewMappedStorage(store, block_size)

	data := make([]byte, block_size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	err = ms.WriteBlock(0x12345678, data)
	assert.NoError(t, err)

	err = ms.WriteBlock(0x1234, data)
	assert.NoError(t, err)

	err = ms.WriteBlock(0x1234, data)
	assert.NoError(t, err)

	err = ms.WriteBlock(0x5678, data)
	assert.ErrorIs(t, err, Err_out_of_space)

	assert.Equal(t, uint64(2*block_size), ms.Size())

}

func TestMappedStorageDupe(t *testing.T) {
	block_size := 4096
	store := sources.NewMemoryStorage(64 * block_size)
	ms := NewMappedStorage(store, block_size)
	// Write some blocks, then read them back

	data := make([]byte, block_size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	err = ms.WriteBlock(0x12345678, data)
	assert.NoError(t, err)

	// Now create a new MappedStorage and make sure we can use it...
	ms2 := NewMappedStorage(store, block_size)
	ms2.SetMap(ms.GetMap())

	buffer := make([]byte, block_size)

	err = ms2.ReadBlock(0x12345678, buffer)
	assert.NoError(t, err)

	assert.Equal(t, buffer, data)

	err = ms2.ReadBlock(0xdead, buffer)
	assert.ErrorIs(t, err, Err_not_found)

	assert.Equal(t, uint64(block_size), ms.Size())

}

func TestMappedStorageGetAddresses(t *testing.T) {
	block_size := 4096
	store := sources.NewMemoryStorage(64 * block_size)

	ms := NewMappedStorage(store, block_size)
	// Write some blocks, then read them back

	data := make([]byte, block_size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	err = ms.WriteBlock(0x12345678, data)
	assert.NoError(t, err)

	err = ms.WriteBlock(0x12345678+uint64(block_size), data)
	assert.NoError(t, err)

	addresses := ms.GetBlockAddresses()
	slices.Sort(addresses)

	assert.Equal(t, []uint64{0x12345678, 0x12346678}, addresses)
}

func TestMappedStorageGetRegions(t *testing.T) {
	block_size := 4096
	store := sources.NewMemoryStorage(64 * block_size)

	ms := NewMappedStorage(store, block_size)
	// Write some blocks, then read them back

	data := make([]byte, block_size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	// Make a region here
	for i := 0; i < 8; i++ {
		err = ms.WriteBlock(0x12345678+uint64(i*block_size), data)
		assert.NoError(t, err)

	}

	// Single block here
	err = ms.WriteBlock(0x20000000, data)
	assert.NoError(t, err)

	addresses := ms.GetBlockAddresses()
	regions := ms.GetRegions(addresses, uint64(block_size*6))

	assert.Equal(t, map[uint64]uint64{
		305419896: 24576,
		305444472: 8192,
		536870912: 4096,
	}, regions)

}

func TestMappedStorageReadWriteBlocks(t *testing.T) {
	block_size := 4096
	store := sources.NewMemoryStorage(64 * block_size)

	ms := NewMappedStorage(store, block_size)
	// Write some blocks, then read them back

	data := make([]byte, block_size*2)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	err = ms.WriteBlocks(0x12345678, data)
	assert.NoError(t, err)

	buffer := make([]byte, block_size)

	err = ms.ReadBlock(0x12345678, buffer)
	assert.NoError(t, err)

	assert.Equal(t, buffer, data[:block_size])

	err = ms.ReadBlock(uint64(0x12345678+block_size), buffer)
	assert.NoError(t, err)
	assert.Equal(t, buffer, data[block_size:])

	buffer2 := make([]byte, block_size*2)
	err = ms.ReadBlocks(0x12345678, buffer2)
	assert.NoError(t, err)

	assert.Equal(t, data, buffer2)
}

func TestDefrag(t *testing.T) {
	block_size := 4096
	store1 := sources.NewMemoryStorage(64 * block_size)
	store2 := sources.NewMemoryStorage(64 * block_size)

	ms1 := NewMappedStorage(store1, block_size)
	ms2 := NewMappedStorage(store2, block_size)

	// Here's some data we're going to write
	data := make([]byte, block_size*2)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	err = ms1.WriteBlock(0x12345678, data[:block_size])
	assert.NoError(t, err)

	other := make([]byte, block_size)
	_, err = rand.Read(other)
	assert.NoError(t, err)

	err = ms1.WriteBlock(0xdead, other)
	assert.NoError(t, err)

	err = ms1.WriteBlock(uint64(0x12345678+block_size), data[block_size:])
	assert.NoError(t, err)

	// Now delete the 'other'

	err = ms1.RemoveBlock(0xdead)
	assert.NoError(t, err)

	// We now have 2 blocks, and a hole in the middle...

	assert.Equal(t, uint64(3*block_size), ms1.ProviderUsedSize()) // It's occupying 3 blocks in the storage.

	err = ms1.DefragTo(ms2)
	assert.NoError(t, err)

	assert.Equal(t, uint64(2*block_size), ms2.ProviderUsedSize()) // It's defragged to 2 blocks.

	// Make sure the data is there
	buffer := make([]byte, block_size*2)

	err = ms2.ReadBlocks(0x12345678, buffer)
	assert.NoError(t, err)

	assert.Equal(t, data, buffer)

	// Delete it...
	err = ms2.RemoveBlocks(0x12345678, uint64(len(buffer)))
	assert.NoError(t, err)

	assert.Equal(t, uint64(0), ms2.Size())
	assert.Equal(t, uint64(0), ms2.ProviderUsedSize())
}

func TestMappedStorageKeepOnly(t *testing.T) {
	block_size := 4096
	store := sources.NewMemoryStorage(64 * block_size)

	ms := NewMappedStorage(store, block_size)
	// Write some blocks, then read them back

	data := make([]byte, block_size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	id1 := uint64(0x12345678)
	err = ms.WriteBlock(id1, data)
	assert.NoError(t, err)

	id2 := uint64(0x80084004)
	err = ms.WriteBlock(id2, data)
	assert.NoError(t, err)

	// Now keep only
	removed := ms.KeepOnly(map[uint64]bool{
		id1: true,
	})

	assert.Equal(t, removed, []uint64{id2})

	assert.Equal(t, uint64(block_size), ms.Size())
}
