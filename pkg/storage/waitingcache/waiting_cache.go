package waitingcache

import (
	"sync"

	"github.com/google/uuid"
	"github.com/loopholelabs/logging/types"
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
	logger           types.Logger
	uuid             uuid.UUID
	prov             storage.Provider
	local            *Local
	remote           *Remote
	writeLock        sync.Mutex
	blockSize        int
	size             uint64
	lockers          map[uint]*sync.RWMutex
	lockersLock      sync.Mutex
	allowLocalWrites bool
}

func NewWaitingCache(prov storage.Provider, blockSize int) (*Local, *Remote) {
	return NewWaitingCacheWithLogger(prov, blockSize, nil)
}

func NewWaitingCacheWithLogger(prov storage.Provider, blockSize int, log types.Logger) (*Local, *Remote) {
	numBlocks := (int(prov.Size()) + blockSize - 1) / blockSize
	wc := &WaitingCache{
		logger:           log,
		uuid:             uuid.New(),
		prov:             prov,
		blockSize:        blockSize,
		size:             prov.Size(),
		lockers:          make(map[uint]*sync.RWMutex),
		allowLocalWrites: true,
	}
	wc.local = &Local{
		wc:         wc,
		available:  *util.NewBitfield(numBlocks),
		NeedAt:     func(_ int64, _ int32) {},
		DontNeedAt: func(_ int64, _ int32) {},
	}
	wc.remote = &Remote{
		wc:        wc,
		available: *util.NewBitfield(numBlocks),
	}
	return wc.local, wc.remote
}

func (i *WaitingCache) waitForBlocks(bStart uint, bEnd uint, lockCB func(b uint)) {
	// TODO: Optimize this
	for b := bStart; b < bEnd; b++ {
		i.waitForBlock(b, lockCB)
	}
}

func (i *WaitingCache) waitForBlock(b uint, lockCB func(b uint)) {
	if i.logger != nil {
		i.logger.Trace().
			Str("uuid", i.uuid.String()).
			Uint("block", b).
			Msg("waitForBlock")
		defer i.logger.Trace().
			Str("uuid", i.uuid.String()).
			Uint("block", b).
			Msg("waitForBlock complete")
	}

	// If we have it locally, return.
	if i.local.available.BitSet(int(b)) {
		return
	}

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

func (i *WaitingCache) markAvailableBlockLocal(b uint) {
	if i.logger != nil {
		i.logger.Trace().
			Str("uuid", i.uuid.String()).
			Uint("block", b).
			Msg("markAvailableLocalBlock")
	}

	i.lockersLock.Lock()
	avail := i.local.available.BitSet(int(b))
	rwl, ok := i.lockers[b]
	if !avail {
		i.local.available.SetBit(int(b))
	}
	i.lockersLock.Unlock()

	if !avail && ok {
		rwl.Unlock()
	}

	// Now we can get rid of the lock...
	i.lockersLock.Lock()
	delete(i.lockers, b)
	i.lockersLock.Unlock()
}

func (i *WaitingCache) markAvailableRemoteBlocks(bStart uint, bEnd uint) {
	// TODO: Optimize this
	for b := bStart; b < bEnd; b++ {
		i.markAvailableRemoteBlock(b)
	}
}

func (i *WaitingCache) markAvailableRemoteBlock(b uint) {
	if i.logger != nil {
		i.logger.Trace().
			Str("uuid", i.uuid.String()).
			Uint("block", b).
			Msg("markAvailableRemoteBlock")
	}

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
	if i.logger != nil {
		i.logger.Trace().
			Str("uuid", i.uuid.String()).
			Uint("block", b).
			Msg("markUnavailableRemoteBlock")
	}

	i.lockersLock.Lock()
	avail := i.remote.available.BitSet(int(b))
	if avail {
		i.remote.available.ClearBit(int(b))
	}
	i.lockersLock.Unlock()
}
