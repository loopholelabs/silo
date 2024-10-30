package waitingcache

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
	prov             storage.StorageProvider
	local            *WaitingCacheLocal
	remote           *WaitingCacheRemote
	writeLock        sync.Mutex
	blockSize        int
	size             uint64
	lockers          map[uint]*sync.RWMutex
	lockersLock      sync.Mutex
	allowLocalWrites bool
}

func NewWaitingCache(prov storage.StorageProvider, blockSize int) (*WaitingCacheLocal, *WaitingCacheRemote) {
	numBlocks := (int(prov.Size()) + blockSize - 1) / blockSize
	wc := &WaitingCache{
		prov:             prov,
		blockSize:        blockSize,
		size:             prov.Size(),
		lockers:          make(map[uint]*sync.RWMutex),
		allowLocalWrites: true,
	}
	wc.local = &WaitingCacheLocal{
		wc:         wc,
		available:  *util.NewBitfield(numBlocks),
		NeedAt:     func(offset int64, length int32) {},
		DontNeedAt: func(offset int64, length int32) {},
	}
	wc.remote = &WaitingCacheRemote{
		wc:        wc,
		available: *util.NewBitfield(numBlocks),
	}
	return wc.local, wc.remote
}

func (i *WaitingCache) waitForRemoteBlocks(bStart uint, bEnd uint, lockCB func(b uint)) {
	// TODO: Optimize this
	for b := bStart; b < bEnd; b++ {
		i.waitForRemoteBlock(b, lockCB)
	}
}

func (i *WaitingCache) waitForRemoteBlock(b uint, lockCB func(b uint)) {
	i.lockersLock.Lock()
	avail := i.remote.available.BitSet(int(b))
	if avail {
		i.lockersLock.Unlock()
		return
	}
	rwl, ok := i.lockers[b]
	if !ok {
		// The first waiter will call .Lock()
		rwl = &sync.RWMutex{}
		i.lockers[b] = rwl
		rwl.Lock() // NB: Non-symetric lock. This will be unlocked by the WriteAt in another thread.
		lockCB(b)
	}
	i.lockersLock.Unlock()

	// Lock for reading (This will wait until the write lock has been unlocked by a writer for this block).
	rwl.RLock()
}

func (i *WaitingCache) markAvailableRemoteBlocks(bStart uint, bEnd uint) {
	// TODO: Optimize this
	for b := bStart; b < bEnd; b++ {
		i.markAvailableRemoteBlock(b)
	}
}

func (i *WaitingCache) markAvailableRemoteBlock(b uint) {
	i.lockersLock.Lock()
	avail := i.remote.available.BitSet(int(b))
	rwl, ok := i.lockers[b]
	if !avail {
		i.remote.available.SetBit(int(b))
	}

	// If we have waiters for it, we can go ahead and unlock to allow them to read it.
	if !avail && ok {
		rwl.Unlock()
	}

	// Now we can get rid of the lock on this block...
	delete(i.lockers, b)
	i.lockersLock.Unlock()
}

func (i *WaitingCache) markUnavailableRemoteBlock(b uint) {
	i.lockersLock.Lock()
	avail := i.remote.available.BitSet(int(b))
	if avail {
		i.remote.available.ClearBit(int(b))
	}
	i.lockersLock.Unlock()
}
