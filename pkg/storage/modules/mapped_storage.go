package modules

import (
	"errors"
	"slices"
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

func (ms *MappedStorage) AppendMap(data map[uint64]uint64) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	for k, v := range data {
		ms.id_to_block[k] = v
		// Set the other bits as well
		ms.block_available.ClearBit(int(v))
		ms.IDs[v] = k
	}
}

func (ms *MappedStorage) GetMapForSourceRange(offset int64, length int) map[uint64]uint64 {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	idmap := make(map[uint64]uint64)
	end := uint64(offset + int64(length))
	if end > ms.prov.Size() {
		end = ms.prov.Size()
	}

	b_start := uint(offset / int64(ms.block_size))
	b_end := uint((end-1)/uint64(ms.block_size)) + 1

	// Now we have the blocks, lets find the IDs...
	for b := b_start; b < b_end; b++ {
		if !ms.block_available.BitSet(int(b)) {
			id := ms.IDs[b]
			idmap[id] = uint64(b)
		}
	}

	return idmap
}

func (ms *MappedStorage) GetMap() map[uint64]uint64 {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	idmap := make(map[uint64]uint64)
	for id, b := range ms.id_to_block {
		idmap[id] = b
	}
	return idmap
}

func (ms *MappedStorage) SetMap(newmap map[uint64]uint64) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	// Clear everything...
	ms.block_available.SetBits(0, ms.block_available.Length())
	ms.IDs = make([]uint64, ms.block_available.Length())
	ms.id_to_block = make(map[uint64]uint64)
	// Now set things...
	for id, b := range newmap {
		ms.IDs[b] = id
		ms.id_to_block[id] = b
		ms.block_available.ClearBit(int(b))
	}
}

func (ms *MappedStorage) Size() uint64 {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	return uint64(len(ms.id_to_block)) * uint64(ms.block_size)
}

func (ms *MappedStorage) ProviderUsedSize() uint64 {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	maxb := uint64(0)
	if len(ms.id_to_block) == 0 {
		return 0
	}
	for _, b := range ms.id_to_block {
		if b > maxb {
			maxb = b
		}
	}
	return (maxb + 1) * uint64(ms.block_size)
}

// Optimized where we can for large blocks
func (ms *MappedStorage) WriteBlocks(id uint64, data []byte) error {
	// First lets check if we have seen this id before, and if the blocks are continuous
	ms.lock.Lock()

	//first_block := uint64(0)
	//ok := false
	//can_do_bulk := false

	first_block, ok := ms.id_to_block[id]
	can_do_bulk := true
	if ok {
		// The first block exists. Make sure all the others exist and are in a line.
		bptr := uint64(0)
		for ptr := 0; ptr < len(data); ptr += ms.block_size {
			bptr++
			block, sok := ms.id_to_block[id+uint64(ptr)]
			if !sok || block != (first_block+bptr) {
				can_do_bulk = false
				break
			}
		}
	} else {
		// The first block doesn't exist. Make sure none of the others do
		for ptr := 0; ptr < len(data); ptr += ms.block_size {
			_, sok := ms.id_to_block[id+uint64(ptr)]
			if sok {
				can_do_bulk = false
			}
		}
	}

	if !can_do_bulk {
		ms.lock.Unlock()
		// We can't do a bulk write
		for ptr := 0; ptr < len(data); ptr += ms.block_size {
			err := ms.WriteBlock(id+uint64(ptr), data[ptr:ptr+ms.block_size])
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Do a bulk write here...
	if ok {
		ms.lock.Unlock()
		// Write some data for this block
		// NB: We can do this outside the lock
		offset := first_block * uint64(ms.block_size)
		_, err := ms.prov.WriteAt(data, int64(offset))
		return err
	}

	// We have no record of any of this data yet. Find some space to store it.
	num_blocks := len(data) / ms.block_size

	for b := 0; b < int(ms.block_available.Length())-num_blocks; b++ {
		// Check if we have some space
		if ms.block_available.BitsSet(uint(b), uint(b+num_blocks)) {

			ms.block_available.ClearBits(uint(b), uint(b+num_blocks))
			// Now do the write outside the lock...

			ptr := uint64(0)
			for i := b; i < b+num_blocks; i++ {
				ms.id_to_block[id+ptr] = uint64(i)
				ms.IDs[i] = id + ptr
				ptr += uint64(ms.block_size)
			}

			ms.lock.Unlock()
			offset := uint64(b) * uint64(ms.block_size)
			_, err := ms.prov.WriteAt(data, int64(offset))
			return err
		}
	}

	ms.lock.Unlock()
	return Err_out_of_space
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

func (ms *MappedStorage) ReadBlocks(id uint64, data []byte) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	return ms.readBlocksInt(id, data)
}

// TODO: Optimize when the data is continuous
func (ms *MappedStorage) readBlocksInt(id uint64, data []byte) error {
	for ptr := 0; ptr < len(data); ptr += ms.block_size {
		b, ok := ms.id_to_block[id+uint64(ptr)]
		if !ok {
			return Err_not_found
		}

		offset := b * uint64(ms.block_size)
		_, err := ms.prov.ReadAt(data[ptr:ptr+ms.block_size], int64(offset))
		if err != nil {
			return err
		}
	}
	return nil
}

/**
 * Write a single block
 *
 */
func (ms *MappedStorage) WriteBlock(id uint64, data []byte) error {
	// First lets check if we have seen this id before...
	ms.lock.Lock()

	b, ok := ms.id_to_block[id]
	if !ok {
		newb, err := ms.block_available.CollectFirstAndClear(0, ms.block_available.Length())
		if err != nil {
			ms.lock.Unlock()
			return Err_out_of_space
		}
		// Init the block
		b = uint64(newb)
		ms.id_to_block[id] = b
		ms.IDs[b] = id
	}

	ms.lock.Unlock()

	// Write some data for this block
	// NB: We can do this outside the lock
	offset := b * uint64(ms.block_size)
	_, err := ms.prov.WriteAt(data, int64(offset))
	return err
}

/**
 * Remove a single block from this MappedStorage device
 * NB: This will create a HOLE in the storage.
 */
func (ms *MappedStorage) RemoveBlock(id uint64) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	b, ok := ms.id_to_block[id]
	if !ok {
		return Err_not_found
	}

	delete(ms.id_to_block, id)
	ms.block_available.SetBit(int(b))
	return nil
}

/**
 * Remove blocks from this MappedStorage device
 * NB: This will create HOLEs in the storage.
 */
func (ms *MappedStorage) RemoveBlocks(id uint64, length uint64) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	for ptr := uint64(0); ptr < length; ptr += uint64(ms.block_size) {
		b, ok := ms.id_to_block[id+ptr]
		if !ok {
			return Err_not_found
		}

		delete(ms.id_to_block, id+ptr)
		ms.block_available.SetBit(int(b))
	}
	return nil
}

/**
 * Get a list of all addresses in the mapped storage
 *
 */
func (ms *MappedStorage) GetBlockAddresses() []uint64 {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	addrs := make([]uint64, 0)
	for id := range ms.id_to_block {
		addrs = append(addrs, id)
	}
	return addrs
}

/**
 * Given a list of addresses, get a map of continuous ranges
 * max_size of 0 means unlimited.
 */
func (ms *MappedStorage) GetRegions(addresses []uint64, max_size uint64) map[uint64]uint64 {
	ranges := make(map[uint64]uint64)

	exists := make(map[uint64]bool)
	slices.Sort(addresses)
	for _, a := range addresses {
		exists[a] = true
	}

	// Go through selecting ranges to use...
	for _, a := range addresses {
		_, ok := exists[a]
		if ok {
			delete(exists, a)

			ranges[a] = uint64(ms.block_size)
			// Try to combine some more...
			ptr := ms.block_size
			for {
				if max_size != 0 && ranges[a] >= max_size {
					break
				}
				// Make sure it exists
				_, mok := exists[a+uint64(ptr)]
				if mok {
					delete(exists, a+uint64(ptr))

					ranges[a] += uint64(ms.block_size)
					ptr += ms.block_size
				} else {
					break
				}
			}
		}
	}

	return ranges
}

/**
 * Defrag into an empty destination
 */
func (ms *MappedStorage) DefragTo(dest *MappedStorage) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	// First get list of USED IDs, and then get regions...
	ids := make([]uint64, 0)
	for id := range ms.id_to_block {
		ids = append(ids, id)
	}

	all_regions := ms.GetRegions(ids, 0)

	region_ids := make([]uint64, 0)
	for id := range all_regions {
		region_ids = append(region_ids, id)
	}

	slices.Sort(region_ids)

	// Now read+write all data to the destination
	for _, id := range region_ids {
		length := all_regions[id]
		data := make([]byte, length)

		// Read the source blocks...
		err := ms.readBlocksInt(id, data)
		if err != nil {
			return err
		}

		err = dest.WriteBlocks(id, data)
		if err != nil {
			return err
		}
	}
	return nil
}
