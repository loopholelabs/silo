package modules

import (
	"sync"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type blockData struct {
	log []int64
}

/**
 * Add a reading to the log
 *
 */
func (bd *blockData) Add(expiry time.Duration) {
	n := time.Now().UnixNano()

	// Either add it on, or replace an expired one...
	for i := 0; i < len(bd.log); i++ {
		if bd.log[i] < n-int64(expiry) {
			bd.log[i] = n
			return
		}
	}
	bd.log = append(bd.log, n)
}

/**
 * Count readings in the log
 *
 */
func (bd *blockData) Count(expiry time.Duration) int {
	n := time.Now().UnixNano()
	count := 0
	for i := 0; i < len(bd.log); i++ {
		if bd.log[i] >= n-int64(expiry) {
			count++
		}
	}
	return count
}

type VolatilityMonitor struct {
	prov            storage.StorageProvider
	expiry          time.Duration
	size            uint64
	num_blocks      int
	block_size      int
	block_data      map[uint]*blockData
	block_data_lock sync.Mutex
	available       util.Bitfield
}

func NewVolatilityMonitor(prov storage.StorageProvider, block_size int, expiry time.Duration) *VolatilityMonitor {
	num_blocks := (int(prov.Size()) + block_size - 1) / block_size
	return &VolatilityMonitor{
		prov:       prov,
		size:       prov.Size(),
		num_blocks: num_blocks,
		block_size: block_size,
		block_data: make(map[uint]*blockData),
		available:  *util.NewBitfield(num_blocks),
		expiry:     expiry,
	}
}

/**
 * Add a block to be monitored
 *
 */
func (i *VolatilityMonitor) BlockAvailable(block int) {
	i.available.SetBit(block)
}

/**
 * Remove a block from monitoring
 *
 */
func (i *VolatilityMonitor) RemoveBlock(block int) {
	i.block_data_lock.Lock()
	delete(i.block_data, uint(block))
	i.block_data_lock.Unlock()
	i.available.ClearBit(block)
}

func (i *VolatilityMonitor) GetNextBlock() int {
	block := -1 // All done
	block_count := 0

	// Find something...
	i.block_data_lock.Lock()
	for n := 0; n < i.num_blocks; n++ {
		if i.available.BitSet(n) {
			bd, ok := i.block_data[uint(n)]
			if !ok {
				block = n
				break
				// No readings... Short cut...
			}
			c := bd.Count(i.expiry)
			if c >= block_count {
				block = int(n)
				block_count = c
			}
		}
	}
	i.block_data_lock.Unlock()

	if block != -1 {
		// Remove it
		i.block_data_lock.Lock()
		delete(i.block_data, uint(block))
		i.block_data_lock.Unlock()
		i.available.ClearBit(block)
	}
	return block
}

func (i *VolatilityMonitor) ReadAt(buffer []byte, offset int64) (int, error) {
	return i.prov.ReadAt(buffer, offset)
}

func (i *VolatilityMonitor) WriteAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	n, err := i.prov.WriteAt(buffer, offset)

	for block := b_start; block < b_end; block++ {
		if i.available.BitSet(int(block)) {
			i.block_data_lock.Lock()
			bd, ok := i.block_data[block]
			if !ok {
				bd = &blockData{log: make([]int64, 0)}
				i.block_data[block] = bd
			}
			bd.Add(i.expiry)
			i.block_data_lock.Unlock()
		}
	}

	return n, err
}

func (i *VolatilityMonitor) Flush() error {
	return i.prov.Flush()
}

func (i *VolatilityMonitor) Size() uint64 {
	return i.prov.Size()
}
