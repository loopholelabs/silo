package dirtytracker

import (
	"sort"
	"sync"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

/**
 * This can track writes so that we can get a list of dirty areas after some period.
 * Tracking is enabled for a block when a Read is performed on the remote for that block.
 * A call to sync() grabs the list of dirty blocks, and resets those to not be monitored.
 */

type DirtyTracker struct {
	prov           storage.StorageProvider
	size           uint64
	block_size     int
	num_blocks     int
	dirty_log      *util.Bitfield
	tracking       *util.Bitfield
	tracking_times map[uint]time.Time
	tracking_lock  sync.Mutex
	write_lock     sync.RWMutex
}

type DirtyTrackerLocal struct {
	dt *DirtyTracker
}

func (dtl *DirtyTrackerLocal) ReadAt(buffer []byte, offset int64) (int, error) {
	return dtl.dt.localReadAt(buffer, offset)
}

func (dtl *DirtyTrackerLocal) WriteAt(buffer []byte, offset int64) (int, error) {
	return dtl.dt.localWriteAt(buffer, offset)
}

func (dtl *DirtyTrackerLocal) Flush() error {
	return dtl.dt.localFlush()
}

func (dtl *DirtyTrackerLocal) Size() uint64 {
	return dtl.dt.localSize()
}

func (dtl *DirtyTrackerLocal) Close() error {
	return dtl.dt.prov.Close()
}

type DirtyTrackerRemote struct {
	dt *DirtyTracker
}

func (dtl *DirtyTrackerRemote) ReadAt(buffer []byte, offset int64) (int, error) {
	return dtl.dt.remoteReadAt(buffer, offset)
}

func (dtl *DirtyTrackerRemote) WriteAt(buffer []byte, offset int64) (int, error) {
	return dtl.dt.remoteWriteAt(buffer, offset)
}

func (dtl *DirtyTrackerRemote) Flush() error {
	return dtl.dt.remoteFlush()
}

func (dtl *DirtyTrackerRemote) Size() uint64 {
	return dtl.dt.remoteSize()
}

func (dtl *DirtyTrackerRemote) Close() error {
	return dtl.dt.prov.Close()
}

func NewDirtyTracker(prov storage.StorageProvider, blockSize int) (*DirtyTrackerLocal, *DirtyTrackerRemote) {
	size := int(prov.Size())
	numBlocks := (size + blockSize - 1) / blockSize
	dt := &DirtyTracker{
		size:           prov.Size(),
		block_size:     blockSize,
		num_blocks:     numBlocks,
		prov:           prov,
		tracking:       util.NewBitfield(numBlocks),
		dirty_log:      util.NewBitfield(numBlocks),
		tracking_times: make(map[uint]time.Time),
	}
	return &DirtyTrackerLocal{dt: dt}, &DirtyTrackerRemote{dt: dt}
}

func (i *DirtyTracker) trackArea(length int64, offset int64) {
	end := uint64(offset + length)
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	// Enable tracking for this area
	i.tracking.SetBits(b_start, b_end)
}

func (i *DirtyTrackerRemote) MeasureDirty() int {
	i.dt.tracking_lock.Lock()
	defer i.dt.tracking_lock.Unlock()
	return len(i.dt.tracking_times)
}

func (i *DirtyTrackerRemote) GetDirtyBlocks(max_age time.Duration, limit int, group_by_shift int, min_changed int) []uint {
	grouped_blocks := make(map[uint][]uint)

	// First look for any dirty blocks past max_age
	i.dt.tracking_lock.Lock()
	for b, t := range i.dt.tracking_times {
		if time.Since(t) > max_age {
			grouped_b := b >> group_by_shift
			v, ok := grouped_blocks[grouped_b]
			if ok {
				grouped_blocks[grouped_b] = append(v, b)
			} else {
				grouped_blocks[grouped_b] = []uint{b}
			}
			if len(grouped_blocks) == limit {
				break
			}
		}
	}
	i.dt.tracking_lock.Unlock()

	// Now we look for changed blocks, and sort by how much.
	if len(grouped_blocks) < limit {
		grouped_blocks_changed := make(map[uint][]uint)
		// Now we also look for extra blocks based on how much they have changed...
		bls := i.dt.dirty_log.Collect(0, uint(i.dt.num_blocks))
		for _, b := range bls {
			grouped_b := b >> group_by_shift
			v, ok := grouped_blocks_changed[grouped_b]
			if ok {
				grouped_blocks_changed[grouped_b] = append(v, b)
			} else {
				grouped_blocks_changed[grouped_b] = []uint{b}
			}
		}

		// Now sort by how much changed
		keys := make([]uint, 0, len(grouped_blocks_changed))
		for key := range grouped_blocks_changed {
			keys = append(keys, key)
		}

		// Sort the blocks by how many sub-blocks are dirty.
		sort.SliceStable(keys, func(i, j int) bool {
			return len(grouped_blocks_changed[keys[i]]) < len(grouped_blocks_changed[keys[j]])
		})

		// Now add them into grouped_blocks if we can...
		for {
			if len(grouped_blocks) == limit {
				break
			}
			if len(keys) == 0 {
				break
			}
			// Pick one out of grouped_blocks_changed, and try to add it
			k := keys[0]
			keys = keys[1:]

			if len(grouped_blocks_changed[k]) >= min_changed {
				// This may overwrite an existing entry which is max_age, but we'll have the same block + others here
				grouped_blocks[k] = grouped_blocks_changed[k]
			}
		}
	}

	// Clear out the tracking data here... It'll get added for tracking again on a readAt()
	i.dt.write_lock.Lock()
	defer i.dt.write_lock.Unlock()

	rblocks := make([]uint, 0)
	for rb, blocks := range grouped_blocks {
		for _, b := range blocks {
			i.dt.tracking.ClearBit(int(b))
			i.dt.dirty_log.ClearBit(int(b))
			i.dt.tracking_lock.Lock()
			delete(i.dt.tracking_times, b)
			i.dt.tracking_lock.Unlock()
		}
		rblocks = append(rblocks, rb)
	}

	return rblocks
}

func (i *DirtyTracker) GetAllDirtyBlocks() *util.Bitfield {
	// Prevent any writes while we do the Sync()
	i.write_lock.Lock()
	defer i.write_lock.Unlock()

	info := i.dirty_log.Clone()

	// Remove the dirty blocks from tracking... (They will get added again when a Read is performed to migrate the data)
	i.tracking.ClearBitsIf(info, 0, uint(i.num_blocks))

	// Clear the dirty log.
	i.dirty_log.Clear()

	blocks := info.Collect(0, info.Length())
	i.tracking_lock.Lock()
	for _, b := range blocks {
		delete(i.tracking_times, b)
	}
	i.tracking_lock.Unlock()

	return info
}

func (i *DirtyTrackerRemote) Sync() *util.Bitfield {
	info := i.dt.GetAllDirtyBlocks()
	return info
}

func (i *DirtyTracker) localReadAt(buffer []byte, offset int64) (int, error) {
	return i.prov.ReadAt(buffer, offset)
}

func (i *DirtyTracker) localWriteAt(buffer []byte, offset int64) (int, error) {
	i.write_lock.RLock()
	defer i.write_lock.RUnlock()

	n, err := i.prov.WriteAt(buffer, offset)

	if err == nil {
		end := uint64(offset + int64(len(buffer)))
		if end > i.size {
			end = i.size
		}

		b_start := uint(offset / int64(i.block_size))
		b_end := uint((end-1)/uint64(i.block_size)) + 1

		i.dirty_log.SetBitsIf(i.tracking, b_start, b_end)

		// Update tracking times for last block write
		i.tracking_lock.Lock()
		now := time.Now()
		for b := b_start; b < b_end; b++ {
			i.tracking_times[b] = now
		}
		i.tracking_lock.Unlock()
	}
	return n, err
}

func (i *DirtyTracker) localFlush() error {
	return i.prov.Flush()
}

func (i *DirtyTracker) localSize() uint64 {
	return i.prov.Size()
}

func (i *DirtyTracker) remoteReadAt(buffer []byte, offset int64) (int, error) {

	// Start tracking dirty on the area we read.
	i.trackArea(int64(len(buffer)), offset)
	// NB: A WriteAt could occur here, which would result in an incorrect dirty marking.
	// TODO: Do something to mitigate this without affecting performance.
	// Note though, that this is still preferable to tracking everything before it's been read for migration.
	n, err := i.prov.ReadAt(buffer, offset)

	return n, err
}

func (i *DirtyTracker) remoteWriteAt(buffer []byte, offset int64) (int, error) {
	return i.prov.WriteAt(buffer, offset)
}

func (i *DirtyTracker) remoteFlush() error {
	return i.prov.Flush()
}

func (i *DirtyTracker) remoteSize() uint64 {
	return i.prov.Size()
}
