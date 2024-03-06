package volatilitymonitor

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
	// TODO: Should probably periodically do a cleanup
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
	total_data      *blockData
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
		total_data: &blockData{log: make([]int64, 0)},
	}
}

func (i *VolatilityMonitor) GetNext() *storage.BlockInfo {
	block := -1 // All done
	block_count := 0

	// Find something...
	i.block_data_lock.Lock()
	defer i.block_data_lock.Unlock()

	for n := 0; n < i.num_blocks; n++ {
		if i.available.BitSet(n) {
			bd, ok := i.block_data[uint(n)]
			c := 0
			if ok {
				c = bd.Count(i.expiry)
			}

			// This is NOT a priority block, it only overrules another non-priority block with a smaller/eq count
			if block == -1 || (c <= block_count) {
				block = int(n)
				block_count = c
			}
		}
	}

	if block != -1 {
		// Remove it
		delete(i.block_data, uint(block))
		i.available.ClearBit(block)
		return &storage.BlockInfo{Block: block}
	} else {
		return storage.BlockInfoFinish
	}

}

/**
 * Add all blocks
 *
 */
func (i *VolatilityMonitor) AddAll() {
	i.available.SetBits(0, uint(i.num_blocks))
}

/**
 * Add a block to be monitored
 *
 */
func (i *VolatilityMonitor) Add(block int) {
	i.available.SetBit(block)
}

/**
 * Remove a block from monitoring
 *
 */
func (i *VolatilityMonitor) Remove(block int) {
	i.block_data_lock.Lock()
	delete(i.block_data, uint(block))
	i.block_data_lock.Unlock()
	i.available.ClearBit(block)
}

/**
 * Get a reading for a specific block
 *
 */
func (i *VolatilityMonitor) GetVolatility(block int) int {
	i.block_data_lock.Lock()
	defer i.block_data_lock.Unlock()
	bd, ok := i.block_data[uint(block)]
	if ok {
		return bd.Count(i.expiry)
	}
	return 0
}

/**
 * Get a total reading
 *
 */
func (i *VolatilityMonitor) GetTotalVolatility() int {
	i.block_data_lock.Lock()
	defer i.block_data_lock.Unlock()
	return i.total_data.Count(i.expiry)
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
		// Always measure this...
		i.total_data.Add(i.expiry) // Add to the total volatility counter
	}

	return n, err
}

func (i *VolatilityMonitor) Flush() error {
	return i.prov.Flush()
}

func (i *VolatilityMonitor) Size() uint64 {
	return i.prov.Size()
}
