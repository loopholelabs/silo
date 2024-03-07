package waitingcache

import "github.com/loopholelabs/silo/pkg/storage/util"

type WaitingCacheLocal struct {
	wc         *WaitingCache
	available  util.Bitfield
	NeedAt     func(offset int64, length int32)
	DontNeedAt func(offset int64, length int32)
}

func (wcl *WaitingCacheLocal) ReadAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > wcl.wc.size {
		end = wcl.wc.size
	}

	b_start := uint(offset / int64(wcl.wc.blockSize))
	b_end := uint((end-1)/uint64(wcl.wc.blockSize)) + 1

	// WAIT until all the data is available.
	wcl.wc.waitForRemoteBlocks(b_start, b_end, func(b uint) {
		// This is called when we are the FIRST reader for a block.
		// This essentially dedupes for heavy read tasks - only a single NeedAt per block will get sent.
		wcl.NeedAt(int64(b)*int64(wcl.wc.blockSize), int32(wcl.wc.blockSize))
	})
	return wcl.wc.prov.ReadAt(buffer, offset)
}

// TODO: Fix the logic here a bit
func (i *WaitingCache) markAvailableBlockLocal(b uint) {
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

func (wcl *WaitingCacheLocal) WriteAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > wcl.wc.size {
		end = wcl.wc.size
	}

	b_start := uint(offset / int64(wcl.wc.blockSize))
	b_end := uint((end-1)/uint64(wcl.wc.blockSize)) + 1

	// If the first block is incomplete, we need to wait for it from remote
	if offset > (int64(b_start) * int64(wcl.wc.blockSize)) {
		wcl.wc.waitForRemoteBlock(b_start, func(b uint) {})
		b_start++
	}
	// If the last block is incomplete, we need to wait for it from remote
	if (end % uint64(wcl.wc.blockSize)) > 0 {
		wcl.wc.waitForRemoteBlock(b_end, func(b uint) {})
		b_end--
	}

	wcl.wc.writeLock.Lock()
	n, err := wcl.wc.prov.WriteAt(buffer, offset)
	if err == nil {
		// Mark the middle blocks as got, and notify the remote we no longer need them
		if b_end > b_start {
			num := int32(0)
			for b := b_start; b < b_end; b++ {
				wcl.wc.markAvailableBlockLocal(b)
				num += int32(wcl.wc.blockSize)
			}
			wcl.DontNeedAt(int64(b_start)*int64(wcl.wc.blockSize), num)
		}
	}
	wcl.wc.writeLock.Unlock()
	return n, err
}

func (wcl *WaitingCacheLocal) Availability() (int, int) {
	num_blocks := (int(wcl.wc.prov.Size()) + wcl.wc.blockSize - 1) / wcl.wc.blockSize
	return wcl.wc.remote.available.Count(0, uint(num_blocks)), num_blocks
}

func (wcl *WaitingCacheLocal) Flush() error {
	return wcl.wc.prov.Flush()
}

func (wcl *WaitingCacheLocal) Size() uint64 {
	return wcl.wc.prov.Size()
}

func (wcl *WaitingCacheLocal) DirtyBlocks(blocks []uint) {
	for _, v := range blocks {
		wcl.wc.markUnavailableRemoteBlock(v)
	}
}
