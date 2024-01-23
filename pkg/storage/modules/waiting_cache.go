package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

/**
 * Waiting cache StorageProvider
 *
 */
type WaitingCache struct {
	prov         storage.StorageProvider
	available    util.Bitfield
	block_size   int
	size         uint64
	lockers      map[uint]*sync.RWMutex
	lockers_lock sync.Mutex
}

func NewWaitingCache(prov storage.StorageProvider, block_size int) *WaitingCache {
	num_blocks := (int(prov.Size()) + block_size - 1) / block_size
	return &WaitingCache{
		prov:       prov,
		available:  *util.NewBitfield(num_blocks),
		block_size: block_size,
		size:       prov.Size(),
		lockers:    make(map[uint]*sync.RWMutex),
	}
}

func (i *WaitingCache) ReadAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

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

	for b := b_start; b < b_end; b++ {
		i.haveBlock(b)
	}

	return n, err
}

func (i *WaitingCache) Flush() error {
	return i.prov.Flush()
}

func (i *WaitingCache) Size() uint64 {
	return i.prov.Size()
}
