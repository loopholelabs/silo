package modules

import (
	"io"
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
	lock_write   sync.Mutex
	available    util.Bitfield
	block_size   int
	size         uint64
	lockers      map[uint]*sync.RWMutex
	lockers_lock sync.Mutex
}

type WaitingCacheLocal struct {
	wc         *WaitingCache
	NeedAt     func(offset int64, length int32)
	DontNeedAt func(offset int64, length int32)
}

func (wcl *WaitingCacheLocal) ReadAt(buffer []byte, offset int64) (int, error) {
	return wcl.wc.localReadAt(buffer, offset, wcl.NeedAt)
}

func (wcl *WaitingCacheLocal) WriteAt(buffer []byte, offset int64) (int, error) {
	return wcl.wc.localWriteAt(buffer, offset, wcl.DontNeedAt)
}

func (wcl *WaitingCacheLocal) Availability() (int, int) {
	num_blocks := (int(wcl.wc.prov.Size()) + wcl.wc.block_size - 1) / wcl.wc.block_size
	return wcl.wc.available.Count(0, uint(num_blocks)), num_blocks
}

func (wcl *WaitingCacheLocal) Flush() error {
	return wcl.wc.localFlush()
}

func (wcl *WaitingCacheLocal) Size() uint64 {
	return wcl.wc.localSize()
}

func (wcl *WaitingCacheLocal) DirtyBlocks(blocks []uint) {
	for _, v := range blocks {
		wcl.wc.haveNotBlock(v)
	}
}

type WaitingCacheRemote struct {
	wc *WaitingCache
}

func (wcl *WaitingCacheRemote) ReadAt(buffer []byte, offset int64) (int, error) {
	return wcl.wc.remoteReadAt(buffer, offset)
}

func (wcl *WaitingCacheRemote) WriteAt(buffer []byte, offset int64) (int, error) {
	return wcl.wc.remoteWriteAt(buffer, offset)
}

func (wcl *WaitingCacheRemote) Flush() error {
	return wcl.wc.remoteFlush()
}

func (wcl *WaitingCacheRemote) Size() uint64 {
	return wcl.wc.remoteSize()
}

func NewWaitingCache(prov storage.StorageProvider, block_size int) (*WaitingCacheLocal, *WaitingCacheRemote) {
	num_blocks := (int(prov.Size()) + block_size - 1) / block_size
	wc := &WaitingCache{
		prov:       prov,
		available:  *util.NewBitfield(num_blocks),
		block_size: block_size,
		size:       prov.Size(),
		lockers:    make(map[uint]*sync.RWMutex),
	}
	return &WaitingCacheLocal{
		wc:         wc,
		NeedAt:     func(offset int64, length int32) {},
		DontNeedAt: func(offset int64, length int32) {},
	}, &WaitingCacheRemote{wc: wc}
}

func (i *WaitingCache) localReadAt(buffer []byte, offset int64, needAt func(offset int64, length int32)) (int, error) {
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
		needAt(offset, int32(len(buffer)))
	}

	// WAIT until all the data is available.
	for b := b_start; b < b_end; b++ {
		i.waitForBlock(b)
	}

	return i.prov.ReadAt(buffer, offset)
}

func (i *WaitingCache) remoteReadAt(buffer []byte, offset int64) (int, error) {
	return 0, io.EOF
}

func (i *WaitingCache) localWriteAt(buffer []byte, offset int64, dontNeedAt func(offset int64, length int32)) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	// If the first block is incomplete, we need to wait for it
	if offset > (int64(b_start) * int64(i.block_size)) {
		i.waitForBlock(b_start)
		b_start++
	}
	// If the last block is incomplete, we need to wait for it
	if (end % uint64(i.block_size)) > 0 {
		i.waitForBlock(b_end)
		b_end--
	}

	i.lock_write.Lock()
	n, err := i.prov.WriteAt(buffer, offset)
	if err == nil {
		// Make the middle blocks as got, and notify the remote we no longer need them
		if b_end > b_start {
			num := int32(0)
			for b := b_start; b < b_end; b++ {
				i.haveBlock(b)
				num += int32(i.block_size)
			}
			dontNeedAt(int64(b_start)*int64(i.block_size), num)
		}
	}
	i.lock_write.Unlock()
	return n, err
}

func (i *WaitingCache) remoteWriteAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	var err error
	var n int
	i.lock_write.Lock()

	align := 0
	// If the first block is incomplete, we won't mark it.
	if offset > (int64(b_start) * int64(i.block_size)) {
		b_start++
		align = int(offset - (int64(b_start) * int64(i.block_size)))
	}
	// If the last block is incomplete, we won't mark it. *UNLESS* It's the last block in the storage
	if (end % uint64(i.block_size)) > 0 {
		if uint64(offset)+uint64(len(buffer)) < i.size {
			b_end--
		}
	}

	i.lockers_lock.Lock()
	avail := i.available.Collect(uint(b_start), uint(b_end))
	i.lockers_lock.Unlock()

	if len(avail) != 0 {
		pbuffer := make([]byte, len(buffer))
		_, err = i.prov.ReadAt(pbuffer, offset)
		if err == nil {
			for _, b := range avail {
				s := align + (int(b-b_start) * i.block_size)
				// Merge the data in. We know these are complete blocks.
				// NB This does modify the callers buffer.
				copy(buffer[s:s+i.block_size], pbuffer[s:s+i.block_size])
			}
		}
	}

	if err == nil {
		n, err = i.prov.WriteAt(buffer, offset)
	}

	if err == nil {
		if b_end > b_start {
			for b := b_start; b < b_end; b++ {
				i.haveBlock(b)
			}
		}
	}
	i.lock_write.Unlock()

	return n, err
}

func (i *WaitingCache) localFlush() error {
	return i.prov.Flush()
}

func (i *WaitingCache) localSize() uint64 {
	return i.prov.Size()
}

func (i *WaitingCache) remoteFlush() error {
	return i.prov.Flush()
}

func (i *WaitingCache) remoteSize() uint64 {
	return i.prov.Size()
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
