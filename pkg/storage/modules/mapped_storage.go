package modules

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

var ErrOutOfSpace = errors.New("out of space")
var ErrNotFound = errors.New("not found")

type MappedStorage struct {
	prov           storage.Provider
	IDs            []uint64
	idToBlock      map[uint64]uint64
	blockAvailable util.Bitfield
	lock           sync.Mutex
	blockSize      int

	// Track some metrics here...
	metricWriteBlock              uint64
	metricWriteBlockTime          uint64
	metricWriteBlocks             uint64
	metricWriteBlocksData         uint64
	metricWriteBlocksTime         uint64
	metricWriteBlocksBulkNew      uint64
	metricWriteBlocksBulkExisting uint64
}

type MappedStorageStats struct {
	WriteBlock              uint64
	WriteBlockTime          time.Duration
	WriteBlocks             uint64
	WriteBlocksData         uint64
	WriteBlocksTime         time.Duration
	WriteBlocksBulkNew      uint64
	WriteBlocksBulkExisting uint64
}

func NewMappedStorage(prov storage.Provider, blockSize int) *MappedStorage {
	numBlocks := (prov.Size() + uint64(blockSize) - 1) / uint64(blockSize)
	available := *util.NewBitfield(int(numBlocks))
	available.SetBits(0, available.Length())

	return &MappedStorage{
		prov:           prov,
		IDs:            make([]uint64, numBlocks),
		idToBlock:      make(map[uint64]uint64),
		blockAvailable: available,
		blockSize:      blockSize,
	}
}

func (ms *MappedStorage) Stats() *MappedStorageStats {
	return &MappedStorageStats{
		WriteBlock:              ms.metricWriteBlock,
		WriteBlockTime:          time.Duration(ms.metricWriteBlockTime),
		WriteBlocks:             ms.metricWriteBlocks,
		WriteBlocksData:         ms.metricWriteBlocksData,
		WriteBlocksTime:         time.Duration(ms.metricWriteBlocksTime),
		WriteBlocksBulkNew:      ms.metricWriteBlocksBulkNew,
		WriteBlocksBulkExisting: ms.metricWriteBlocksBulkExisting,
	}
}

func (ms *MappedStorage) ResetStats() {
	atomic.StoreUint64(&ms.metricWriteBlock, 0)
	atomic.StoreUint64(&ms.metricWriteBlockTime, 0)
	atomic.StoreUint64(&ms.metricWriteBlocks, 0)
	atomic.StoreUint64(&ms.metricWriteBlocksData, 0)
	atomic.StoreUint64(&ms.metricWriteBlocksTime, 0)
	atomic.StoreUint64(&ms.metricWriteBlocksBulkNew, 0)
	atomic.StoreUint64(&ms.metricWriteBlocksBulkExisting, 0)
}

func (ms *MappedStorage) AppendMap(data map[uint64]uint64) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	for k, v := range data {
		ms.idToBlock[k] = v
		// Set the other bits as well
		ms.blockAvailable.ClearBit(int(v))
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

	bStart := uint(offset / int64(ms.blockSize))
	bEnd := uint((end-1)/uint64(ms.blockSize)) + 1

	// Now we have the blocks, lets find the IDs...
	for b := bStart; b < bEnd; b++ {
		if !ms.blockAvailable.BitSet(int(b)) {
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
	for id, b := range ms.idToBlock {
		idmap[id] = b
	}
	return idmap
}

func (ms *MappedStorage) SetMap(newmap map[uint64]uint64) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	// Clear everything...
	ms.blockAvailable.SetBits(0, ms.blockAvailable.Length())
	ms.IDs = make([]uint64, ms.blockAvailable.Length())
	ms.idToBlock = make(map[uint64]uint64)
	// Now set things...
	for id, b := range newmap {
		ms.IDs[b] = id
		ms.idToBlock[id] = b
		ms.blockAvailable.ClearBit(int(b))
	}
}

func (ms *MappedStorage) Size() uint64 {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	return uint64(len(ms.idToBlock)) * uint64(ms.blockSize)
}

func (ms *MappedStorage) ProviderUsedSize() uint64 {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	maxb := uint64(0)
	if len(ms.idToBlock) == 0 {
		return 0
	}
	for _, b := range ms.idToBlock {
		if b > maxb {
			maxb = b
		}
	}
	return (maxb + 1) * uint64(ms.blockSize)
}

// Optimized where we can for large blocks
func (ms *MappedStorage) WriteBlocks(id uint64, data []byte) error {
	ctime := time.Now()
	defer func() {
		atomic.AddUint64(&ms.metricWriteBlocks, 1)
		atomic.AddUint64(&ms.metricWriteBlocksData, uint64(len(data)))
		atomic.AddUint64(&ms.metricWriteBlocksTime, uint64(time.Since(ctime)))
	}()

	// First lets check if we have seen this id before, and if the blocks are continuous
	ms.lock.Lock()

	// first_block := uint64(0)
	// ok := false
	// can_do_bulk := false

	firstBlock, ok := ms.idToBlock[id]
	canDoBulk := true
	if len(data) > ms.blockSize {
		if ok {
			// The first block exists. Make sure all the others exist and are in a line.
			bptr := uint64(0)
			for ptr := ms.blockSize; ptr < len(data); ptr += ms.blockSize {
				bptr++
				block, sok := ms.idToBlock[id+uint64(ptr)]
				if !sok || block != (firstBlock+bptr) {
					canDoBulk = false
					break
				}
			}
		} else {
			// The first block doesn't exist. Make sure none of the others do
			for ptr := ms.blockSize; ptr < len(data); ptr += ms.blockSize {
				_, sok := ms.idToBlock[id+uint64(ptr)]
				if sok {
					canDoBulk = false
				}
			}
		}
	}

	if !canDoBulk {
		ms.lock.Unlock()
		// We can't do a bulk write. Fallback on writing blocks individually

		numBlocks := (len(data) + ms.blockSize - 1) / ms.blockSize
		errchan := make(chan error, numBlocks)

		for ptr := 0; ptr < len(data); ptr += ms.blockSize {
			go func(writePtr uint64) {
				errchan <- ms.WriteBlock(id+writePtr, data[writePtr:writePtr+uint64(ms.blockSize)])
			}(uint64(ptr))
		}

		// Wait, and read any errors...
		for n := 0; n < numBlocks; n++ {
			err := <-errchan
			if err != nil {
				return err
			}
		}

		return nil
	}

	// Do a bulk write here...
	if ok {
		ms.lock.Unlock()
		// Write some data for these blocks
		// NB: We can do this outside the lock
		offset := firstBlock * uint64(ms.blockSize)
		_, err := ms.prov.WriteAt(data, int64(offset))
		atomic.AddUint64(&ms.metricWriteBlocksBulkExisting, 1)
		return err
	}

	// We have no record of any of this data yet. Find some space to store it.
	numBlocks := len(data) / ms.blockSize

	for b := 0; b < int(ms.blockAvailable.Length())-numBlocks; b++ {
		// Check if we have some space
		if ms.blockAvailable.BitsSet(uint(b), uint(b+numBlocks)) {

			ms.blockAvailable.ClearBits(uint(b), uint(b+numBlocks))

			ptr := uint64(0)
			for i := b; i < b+numBlocks; i++ {
				ms.idToBlock[id+ptr] = uint64(i)
				ms.IDs[i] = id + ptr
				ptr += uint64(ms.blockSize)
			}

			// Now do the write outside the lock...

			ms.lock.Unlock()
			offset := uint64(b) * uint64(ms.blockSize)
			_, err := ms.prov.WriteAt(data, int64(offset))
			atomic.AddUint64(&ms.metricWriteBlocksBulkNew, 1)
			return err
		}
	}

	ms.lock.Unlock()
	return ErrOutOfSpace
}

/**
 * Read a single block
 *
 */
func (ms *MappedStorage) ReadBlock(id uint64, data []byte) error {
	// First lets check if we have seen this id before...
	ms.lock.Lock()
	defer ms.lock.Unlock()

	b, ok := ms.idToBlock[id]
	if !ok {
		return ErrNotFound
	}

	offset := b * uint64(ms.blockSize)
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
	for ptr := 0; ptr < len(data); ptr += ms.blockSize {
		b, ok := ms.idToBlock[id+uint64(ptr)]
		if !ok {
			return ErrNotFound
		}

		offset := b * uint64(ms.blockSize)
		_, err := ms.prov.ReadAt(data[ptr:ptr+ms.blockSize], int64(offset))
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
	ctime := time.Now()
	defer func() {
		atomic.AddUint64(&ms.metricWriteBlock, 1)
		atomic.AddUint64(&ms.metricWriteBlockTime, uint64(time.Since(ctime)))
	}()

	// First lets check if we have seen this id before...
	ms.lock.Lock()

	b, ok := ms.idToBlock[id]
	if !ok {
		newb, err := ms.blockAvailable.CollectFirstAndClear(0, ms.blockAvailable.Length())
		if err != nil {
			ms.lock.Unlock()
			return ErrOutOfSpace
		}
		// Init the block
		b = uint64(newb)
		ms.idToBlock[id] = b
		ms.IDs[b] = id
	}

	ms.lock.Unlock()

	// Write some data for this block
	// NB: We can do this outside the lock
	offset := b * uint64(ms.blockSize)
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

	b, ok := ms.idToBlock[id]
	if !ok {
		return ErrNotFound
	}

	delete(ms.idToBlock, id)
	ms.blockAvailable.SetBit(int(b))
	return nil
}

/**
 * Remove blocks from this MappedStorage device
 * NB: This will create HOLEs in the storage.
 */
func (ms *MappedStorage) RemoveBlocks(id uint64, length uint64) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	for ptr := uint64(0); ptr < length; ptr += uint64(ms.blockSize) {
		b, ok := ms.idToBlock[id+ptr]
		if !ok {
			return ErrNotFound
		}

		delete(ms.idToBlock, id+ptr)
		ms.blockAvailable.SetBit(int(b))
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
	for id := range ms.idToBlock {
		addrs = append(addrs, id)
	}
	return addrs
}

/**
 * Keep only these ids within the MappedStorage
 *
 */
func (ms *MappedStorage) KeepOnly(ids map[uint64]bool) []uint64 {
	removed := make([]uint64, 0)
	ms.lock.Lock()
	defer ms.lock.Unlock()
	for id, b := range ms.idToBlock {
		_, ok := ids[id]
		if !ok {
			// Remove the block and make it available again
			delete(ms.idToBlock, id)
			ms.blockAvailable.SetBit(int(b))
			removed = append(removed, id)
		}
	}
	return removed
}

/**
 * Given a list of addresses, get a map of continuous ranges
 * max_size of 0 means unlimited.
 */
func (ms *MappedStorage) GetRegions(addresses []uint64, maxSize uint64) map[uint64]uint64 {
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

			ranges[a] = uint64(ms.blockSize)
			// Try to combine some more...
			ptr := ms.blockSize
			for {
				if maxSize != 0 && ranges[a] >= maxSize {
					break
				}
				// Make sure it exists
				_, mok := exists[a+uint64(ptr)]
				if mok {
					delete(exists, a+uint64(ptr))

					ranges[a] += uint64(ms.blockSize)
					ptr += ms.blockSize
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
	for id := range ms.idToBlock {
		ids = append(ids, id)
	}

	allRegions := ms.GetRegions(ids, 0)

	regionIDs := make([]uint64, 0)
	for id := range allRegions {
		regionIDs = append(regionIDs, id)
	}

	slices.Sort(regionIDs)

	// Now read+write all data to the destination
	for _, id := range regionIDs {
		length := allRegions[id]
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
