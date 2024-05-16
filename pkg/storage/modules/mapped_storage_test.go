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
