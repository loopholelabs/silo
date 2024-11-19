package waitingcache

import (
	"sync"
	"sync/atomic"
	"time"

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
	logger                         types.RootLogger
	uuid                           uuid.UUID
	prov                           storage.Provider
	local                          *Local
	remote                         *Remote
	writeLock                      sync.Mutex
	blockSize                      int
	size                           uint64
	lockers                        map[uint]*sync.RWMutex
	lockersLock                    sync.Mutex
	allowLocalWrites               bool
	metricWaitForBlock             uint64
	metricWaitForBlockHadRemote    uint64
	metricWaitForBlockHadLocal     uint64
	metricWaitForBlockTime         uint64
	metricWaitForBlockLock         uint64
	metricWaitForBlockLockDone     uint64
	metricMarkAvailableLocalBlock  uint64
	metricMarkAvailableRemoteBlock uint64
}

type Metrics struct {
	WaitForBlock             uint64
	WaitForBlockHadRemote    uint64
	WaitForBlockHadLocal     uint64
	WaitForBlockTime         time.Duration
	WaitForBlockLock         uint64
	WaitForBlockLockDone     uint64
	MarkAvailableLocalBlock  uint64
	MarkAvailableRemoteBlock uint64
	AvailableLocal           uint64
	AvailableRemote          uint64
}

func NewWaitingCache(prov storage.Provider, blockSize int) (*Local, *Remote) {
	return NewWaitingCacheWithLogger(prov, blockSize, nil)
}

func NewWaitingCacheWithLogger(prov storage.Provider, blockSize int, log types.RootLogger) (*Local, *Remote) {
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

func (i *WaitingCache) GetMetrics() *Metrics {
	return &Metrics{
		WaitForBlock:             atomic.LoadUint64(&i.metricWaitForBlock),
		WaitForBlockHadRemote:    atomic.LoadUint64(&i.metricWaitForBlockHadRemote),
		WaitForBlockHadLocal:     atomic.LoadUint64(&i.metricWaitForBlockHadLocal),
		WaitForBlockTime:         time.Duration(atomic.LoadUint64(&i.metricWaitForBlockTime)),
		WaitForBlockLock:         atomic.LoadUint64(&i.metricWaitForBlockLock),
		WaitForBlockLockDone:     atomic.LoadUint64(&i.metricWaitForBlockLockDone),
		MarkAvailableLocalBlock:  atomic.LoadUint64(&i.metricMarkAvailableLocalBlock),
		MarkAvailableRemoteBlock: atomic.LoadUint64(&i.metricMarkAvailableRemoteBlock),
		AvailableLocal:           uint64(i.local.available.Count(0, i.local.available.Length())),
		AvailableRemote:          uint64(i.remote.available.Count(0, i.local.available.Length())),
	}
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
	atomic.AddUint64(&i.metricWaitForBlock, 1)

	i.lockersLock.Lock()

	// If we have it locally, return.
	if i.local.available.BitSet(int(b)) {
		i.lockersLock.Unlock()
		atomic.AddUint64(&i.metricWaitForBlockHadLocal, 1)
		return
	}

	// If we have it remote, return.
	if i.remote.available.BitSet(int(b)) {
		i.lockersLock.Unlock()
		atomic.AddUint64(&i.metricWaitForBlockHadRemote, 1)
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
	atomic.AddUint64(&i.metricWaitForBlockLock, 1)
	ctime := time.Now()
	rwl.RLock()
	atomic.AddUint64(&i.metricWaitForBlockTime, uint64(time.Since(ctime)))
	atomic.AddUint64(&i.metricWaitForBlockLockDone, 1)
}

func (i *WaitingCache) markAvailableBlockLocal(b uint) {
	if i.logger != nil {
		i.logger.Trace().
			Str("uuid", i.uuid.String()).
			Uint("block", b).
			Msg("markAvailableLocalBlock")
	}
	atomic.AddUint64(&i.metricMarkAvailableLocalBlock, 1)

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
	atomic.AddUint64(&i.metricMarkAvailableRemoteBlock, 1)

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
