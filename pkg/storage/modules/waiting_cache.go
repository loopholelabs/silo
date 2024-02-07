package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

/**
 * Waiting cache StorageProvider
 *
 * NB: This tracks COMPLETE blocks only. Not partial ones.
 *
 */
type WaitingCache struct {
	prov         storage.StorageProvider
	available    util.Bitfield
	block_size   int
	size         uint64
	lockers      map[uint]*sync.RWMutex
	lockers_lock sync.Mutex
	NeedAt       func(offset int64, length int32)
}

func NewWaitingCache(prov storage.StorageProvider, block_size int) *WaitingCache {
	num_blocks := (int(prov.Size()) + block_size - 1) / block_size
	return &WaitingCache{
		prov:       prov,
		available:  *util.NewBitfield(num_blocks),
		block_size: block_size,
		size:       prov.Size(),
		lockers:    make(map[uint]*sync.RWMutex),
		NeedAt:     func(offset int64, length int32) {},
	}
}

func (i *WaitingCache) ReadAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	// Check if we have all the data required
	i.lockers_lock.Lock()
	avail := i.available.BitsSet(uint(b_start), uint(b_end))
	i.lockers_lock.Unlock()

	if !avail {
		// Send a request...
		i.NeedAt(offset, int32(len(buffer)))
	}

	// WAIT until all the data is available.
	for b := b_start; b < b_end; b++ {
		i.waitForBlock(b)
	}

	return i.prov.ReadAt(buffer, offset)
}

func (i *WaitingCache) haveBlock(b uint) {
	i.lockers_lock.Lock()
	avail := i.available.BitSet(int(b))
	rwl, ok := i.lockers[b]
	if !avail {
		i.available.SetBit(int(b))
	}
	i.lockers_lock.Unlock()

	if !avail && ok {
		rwl.Unlock()
	}

	// Now we can get rid of the lock...
	i.lockers_lock.Lock()
	delete(i.lockers, b)
	i.lockers_lock.Unlock()
}

func (i *WaitingCache) haveNotBlock(b uint) {
	i.lockers_lock.Lock()
	avail := i.available.BitSet(int(b))
	if avail {
		i.available.ClearBit(int(b))
	}
	i.lockers_lock.Unlock()
}

func (i *WaitingCache) waitForBlock(b uint) {
	i.lockers_lock.Lock()
	avail := i.available.BitSet(int(b))
	if avail {
		i.lockers_lock.Unlock()
		return
	}
	rwl, ok := i.lockers[b]
	if !ok {
		rwl = &sync.RWMutex{}
		i.lockers[b] = rwl
		rwl.Lock() // NB: Non-symetric lock. This will be unlocked by the WriteAt in another thread.
	}
	i.lockers_lock.Unlock()

	// Lock for reading (This will wait until the write lock has been unlocked by a writer for this block).
	rwl.RLock()
}

func (i *WaitingCache) WriteAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	n, err := i.prov.WriteAt(buffer, offset)

	// If the first block is incomplete, we won't mark it.
	if offset > (int64(b_start) * int64(i.block_size)) {
		b_start++
	}
	// If the last block is incomplete, we won't mark it.
	if (end % uint64(i.block_size)) > 0 {
		b_end--
	}

	if b_end > b_start {
		for b := b_start; b < b_end; b++ {
			i.haveBlock(b)
		}
	}

	return n, err
}

func (i *WaitingCache) DirtyBlocks(blocks []uint) {
	for _, v := range blocks {
		i.haveNotBlock(v)
	}
}

func (i *WaitingCache) Flush() error {
	return i.prov.Flush()
}

func (i *WaitingCache) Size() uint64 {
	return i.prov.Size()
}
