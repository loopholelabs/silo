package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

/**
 * This can track writes so that we can get a list of dirty areas after some period.
 * Tracking is enabled for a block when a Read is performed on that block
 */

type FilterReadDirtyTracker struct {
	track_detail         bool
	prov                 storage.StorageProvider
	size                 uint64
	block_size           int
	num_blocks           int
	dirty_log            *util.Bitfield
	tracking             *util.Bitfield
	tracking_detail_lock sync.Mutex
	tracking_detail      map[int]*util.Bitfield // block_no -> bitfield
}

type SyncData struct {
	ChangedBlocks *util.Bitfield
	ChangedData   map[int]*util.Bitfield
}

func NewFilterReadDirtyTracker(prov storage.StorageProvider, block_size int) *FilterReadDirtyTracker {
	size := int(prov.Size())
	num_blocks := (size + block_size - 1) / block_size
	return &FilterReadDirtyTracker{
		size:            prov.Size(),
		block_size:      block_size,
		num_blocks:      num_blocks,
		prov:            prov,
		tracking:        util.NewBitfield(num_blocks),
		dirty_log:       util.NewBitfield(num_blocks),
		tracking_detail: make(map[int]*util.Bitfield),
		track_detail:    true,
	}
}

func (i *FilterReadDirtyTracker) Sync() *util.Bitfield {
	info := i.dirty_log.Clone()
	i.dirty_log.Clear()

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
	b_end := uint((end-1)/uint64(i.block_size)) + 1

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
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	i.dirty_log.SetBitsIf(i.tracking, b_start, b_end)

	if i.track_detail {
		org := make([]byte, len(buffer))
		n, err := i.prov.ReadAt(org, offset)
		if n == len(org) && err == nil {
			// TODO: Go through the data, and set bits if the data changed...
			for b := b_start; b < b_end; b++ {

			}
		}
	}

	return i.prov.WriteAt(buffer, offset)
}

func (i *FilterReadDirtyTracker) Flush() error {
	return i.prov.Flush()
}

func (i *FilterReadDirtyTracker) Size() uint64 {
	return i.prov.Size()
}
