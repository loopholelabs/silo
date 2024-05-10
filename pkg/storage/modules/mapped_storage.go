package modules

import (
	"errors"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

var Err_out_of_space = errors.New("out of space")
var Err_not_found = errors.New("not found")

type MappedStorage struct {
	prov            storage.StorageProvider
	IDs             []uint64
	id_to_block     map[uint64]uint64
	block_available util.Bitfield
	lock            sync.Mutex
	block_size      int
}

func NewMappedStorage(prov storage.StorageProvider, block_size int) *MappedStorage {
	num_blocks := (prov.Size() + uint64(block_size) - 1) / uint64(block_size)
	available := *util.NewBitfield(int(num_blocks))
	available.SetBits(0, available.Length())

	return &MappedStorage{
		prov:            prov,
		IDs:             make([]uint64, num_blocks),
		id_to_block:     make(map[uint64]uint64),
		block_available: available,
		block_size:      block_size,
	}
}

func (ms *MappedStorage) Size() uint64 {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	return uint64(len(ms.id_to_block)) * uint64(ms.block_size)
}

func (ms *MappedStorage) ReadBlocks(id uint64, data []byte) error {
	for ptr := 0; ptr < len(data); ptr += ms.block_size {
		err := ms.ReadBlock(id+uint64(ptr), data[ptr:ptr+ms.block_size])
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MappedStorage) WriteBlocks(id uint64, data []byte) error {
	for ptr := 0; ptr < len(data); ptr += ms.block_size {
		err := ms.WriteBlock(id+uint64(ptr), data[ptr:ptr+ms.block_size])
		if err != nil {
			return err
		}
	}
	return nil
}

/**
 * Read a single block
 *
 */
func (ms *MappedStorage) ReadBlock(id uint64, data []byte) error {
	// First lets check if we have seen this id before...
	ms.lock.Lock()
	defer ms.lock.Unlock()

	b, ok := ms.id_to_block[id]
	if !ok {
		return Err_not_found
	}

	offset := b * uint64(ms.block_size)
	_, err := ms.prov.ReadAt(data, int64(offset))
	return err
}

/**
 * Write a single block
 *
 */
func (ms *MappedStorage) WriteBlock(id uint64, data []byte) error {
	// First lets check if we have seen this id before...
	ms.lock.Lock()
	defer ms.lock.Unlock()

	b, ok := ms.id_to_block[id]
	if !ok {
		newb, err := ms.block_available.CollectFirstAndClear(0, ms.block_available.Length())
		if err != nil {
			return Err_out_of_space
		}
		// Init the block
		b = uint64(newb)
		ms.id_to_block[id] = b
		ms.IDs[b] = id
	}

	// Write some data for this block
	offset := b * uint64(ms.block_size)
	_, err := ms.prov.WriteAt(data, int64(offset))
	return err
}

/**
 * Remove a single block from this MappedStorage device
 *
 */
func (ms *MappedStorage) RemoveBlock(id uint64) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	b, ok := ms.id_to_block[id]
	if !ok {
		return Err_not_found
	}

	delete(ms.id_to_block, id)
	ms.block_available.ClearBit(int(b))
	return nil
}
