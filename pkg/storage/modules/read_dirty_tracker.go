package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

/**
 * This can track writes so that we can get a list of dirty areas after some period.
 * Tracking is enabled for a block when a Read is performed
 */

type FilterReadDirtyTracker struct {
	prov       storage.StorageProvider
	size       uint64
	block_size int
	num_blocks int
	dirty_log  *util.Bitfield
	tracking   *util.Bitfield
}

func NewFilterReadDirtyTracker(prov storage.StorageProvider, block_size int) *FilterReadDirtyTracker {
	size := int(prov.Size())
	num_blocks := (size + block_size - 1) / block_size
	return &FilterReadDirtyTracker{
		size:       prov.Size(),
		block_size: block_size,
		num_blocks: num_blocks,
		prov:       prov,
		tracking:   util.NewBitfield(num_blocks),
		dirty_log:  util.NewBitfield(num_blocks),
	}
}

func (i *FilterReadDirtyTracker) Sync() *util.Bitfield {
	info := i.dirty_log
	i.dirty_log = util.NewBitfield(i.num_blocks)

	// Remove these blocks from tracking...
	i.tracking.ClearBitsIf(info, 0, uint(i.num_blocks))
	return info
}

func (i *FilterReadDirtyTracker) ReadAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint(end / uint64(i.block_size))
	if b_start == b_end {
		b_end++
	}

	// Enable tracking for this area
	i.tracking.SetBits(b_start, b_end)
	return i.prov.ReadAt(buffer, offset)
}

func (i *FilterReadDirtyTracker) WriteAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint(end / uint64(i.block_size))
	if b_start == b_end {
		b_end++
	}

	i.dirty_log.SetBitsIf(i.tracking, b_start, b_end)

	return i.prov.WriteAt(buffer, offset)
}

func (i *FilterReadDirtyTracker) Flush() error {
	return i.prov.Flush()
}

func (i *FilterReadDirtyTracker) Size() uint64 {
	return i.prov.Size()
}
