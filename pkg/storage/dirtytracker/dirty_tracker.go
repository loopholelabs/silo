package dirtytracker

import (
	"fmt"
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
	prov          storage.Provider
	size          uint64
	blockSize     int
	numBlocks     int
	dirtyLog      *util.Bitfield
	tracking      *util.Bitfield
	trackingTimes map[uint]time.Time
	trackingLock  sync.Mutex
	writeLock     sync.RWMutex
}

type Metrics struct {
	BlockSize      uint64
	Size           uint64
	TrackingBlocks uint64
	DirtyBlocks    uint64
	MaxAgeDirty    time.Duration
}

func (dtr *Remote) GetMetrics() *Metrics {
	// Figure out oldest dirty block...
	dtr.dt.trackingLock.Lock()
	maxAgeDirty := time.Duration(0)
	minAge := time.Now()
	for _, t := range dtr.dt.trackingTimes {
		if t.Before(minAge) {
			minAge = t
			maxAgeDirty = time.Since(minAge)
		}
	}
	dtr.dt.trackingLock.Unlock()

	return &Metrics{
		BlockSize:      uint64(dtr.dt.blockSize),
		Size:           dtr.dt.size,
		TrackingBlocks: uint64(dtr.dt.tracking.Count(0, dtr.dt.tracking.Length())),
		DirtyBlocks:    uint64(dtr.dt.dirtyLog.Count(0, dtr.dt.dirtyLog.Length())),
		MaxAgeDirty:    maxAgeDirty,
	}
}

type Local struct {
	storage.ProviderWithEvents
	dt *DirtyTracker
}

// Relay events to embedded StorageProvider
func (dtl *Local) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := dtl.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(dtl.dt.prov, eventType, eventData)...)
}

func (dtl *Local) ReadAt(buffer []byte, offset int64) (int, error) {
	return dtl.dt.localReadAt(buffer, offset)
}

func (dtl *Local) WriteAt(buffer []byte, offset int64) (int, error) {
	return dtl.dt.localWriteAt(buffer, offset)
}

func (dtl *Local) Flush() error {
	return dtl.dt.localFlush()
}

func (dtl *Local) Size() uint64 {
	return dtl.dt.localSize()
}

func (dtl *Local) Close() error {
	return dtl.dt.prov.Close()
}

func (dtl *Local) CancelWrites(offset int64, length int64) {
	dtl.dt.prov.CancelWrites(offset, length)
}

type Remote struct {
	storage.ProviderWithEvents
	dt *DirtyTracker
}

// Relay events to embedded StorageProvider
func (dtr *Remote) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := dtr.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(dtr.dt.prov, eventType, eventData)...)
}

func (dtr *Remote) ReadAt(buffer []byte, offset int64) (int, error) {
	return dtr.dt.remoteReadAt(buffer, offset)
}

func (dtr *Remote) WriteAt(buffer []byte, offset int64) (int, error) {
	return dtr.dt.remoteWriteAt(buffer, offset)
}

func (dtr *Remote) Flush() error {
	return dtr.dt.remoteFlush()
}

func (dtr *Remote) Size() uint64 {
	return dtr.dt.remoteSize()
}

func (dtr *Remote) Close() error {
	return dtr.dt.prov.Close()
}

func (dtr *Remote) CancelWrites(offset int64, length int64) {
	dtr.dt.prov.CancelWrites(offset, length)
}

func NewDirtyTracker(prov storage.Provider, blockSize int) (*Local, *Remote) {
	size := int(prov.Size())
	numBlocks := (size + blockSize - 1) / blockSize
	dt := &DirtyTracker{
		size:          prov.Size(),
		blockSize:     blockSize,
		numBlocks:     numBlocks,
		prov:          prov,
		tracking:      util.NewBitfield(numBlocks),
		dirtyLog:      util.NewBitfield(numBlocks),
		trackingTimes: make(map[uint]time.Time),
	}
	return &Local{dt: dt}, &Remote{dt: dt}
}

func (dt *DirtyTracker) trackArea(length int64, offset int64) {
	end := uint64(offset + length)
	if end > dt.size {
		end = dt.size
	}

	bStart := uint(offset / int64(dt.blockSize))
	bEnd := uint((end-1)/uint64(dt.blockSize)) + 1

	// Enable tracking for this area
	dt.tracking.SetBits(bStart, bEnd)
}

/**
 * Ask downstream if there are blocks that aren't required for a migration.
 * Typically these would be a base overlay, but could be something else.
 * As well as returning the blocks, this call will update the dirty tracker as if the blocks had been read (To start tracking dirty changes)
 */
func (dtr *Remote) GetUnrequiredBlocks() []uint {
	// Make sure no writes get through...
	dtr.dt.writeLock.Lock()
	defer dtr.dt.writeLock.Unlock()

	// Snapshot blocks from any CoW, and update the tracking
	cowBlocks := storage.SendSiloEvent(dtr.dt.prov, storage.EventTypeCowGetBlocks, dtr)
	if len(cowBlocks) == 1 {
		blocks := cowBlocks[0].([]uint)
		for _, b := range blocks {
			offset := b * uint(dtr.dt.blockSize)
			length := uint(dtr.dt.blockSize)

			// NB overflow here shouldn't matter. TrackArea will cope and truncate it.
			dtr.dt.trackArea(int64(length), int64(offset))
		}
		return blocks
	}
	return nil
}

/**
 * Start tracking at the given offset and length
 *
 */
func (dtr *Remote) TrackAt(length int64, offset int64) {
	dtr.dt.trackArea(length, offset)
}

/**
 * Check which blocks are being tracked
 *
 */
func (dtr *Remote) GetTrackedBlocks() []uint {
	return dtr.dt.tracking.Collect(0, dtr.dt.tracking.Length())
}

/**
 * Get a quick measure of how many blocks are currently dirty
 *
 */
func (dtr *Remote) MeasureDirty() int {
	dtr.dt.trackingLock.Lock()
	defer dtr.dt.trackingLock.Unlock()

	fmt.Printf("Dirty blocks=%d size=%d %d / %d\n", dtr.dt.numBlocks, dtr.dt.size, dtr.dt.tracking.Count(0, uint(dtr.dt.numBlocks)), dtr.dt.dirtyLog.Count(0, uint(dtr.dt.numBlocks)))

	return len(dtr.dt.trackingTimes)
}

/**
 * Get a quick measure of the oldest dirty block
 *
 */
func (dtr *Remote) MeasureDirtyAge() time.Time {
	dtr.dt.trackingLock.Lock()
	defer dtr.dt.trackingLock.Unlock()
	minAge := time.Now()
	for _, t := range dtr.dt.trackingTimes {
		if t.Before(minAge) {
			minAge = t
		}
	}
	return minAge
}

/**
 * Get some dirty blocks using the given criteria
 *
 * - max_age - Returns blocks older than the given duration as priority
 * - limit -   Limits how many blocks are returned
 * - group_by_shift - Groups subblocks into blocks using the given shift
 * - min_changed    - Minimum subblock changes in a block
 */
func (dtr *Remote) GetDirtyBlocks(maxAge time.Duration, limit int, groupByShift int, minChanged int) []uint {
	// Prevent any writes while we get dirty blocks
	dtr.dt.writeLock.Lock()
	defer dtr.dt.writeLock.Unlock()

	groupedBlocks := make(map[uint][]uint)

	// First look for any dirty blocks past max_age
	dtr.dt.trackingLock.Lock()
	for b, t := range dtr.dt.trackingTimes {
		if time.Since(t) > maxAge {
			groupedB := b >> groupByShift
			v, ok := groupedBlocks[groupedB]
			if ok {
				groupedBlocks[groupedB] = append(v, b)
			} else {
				groupedBlocks[groupedB] = []uint{b}
			}
			if len(groupedBlocks) == limit {
				break
			}
		}
	}
	dtr.dt.trackingLock.Unlock()

	// Now we look for changed blocks, and sort by how much.
	if len(groupedBlocks) < limit {
		groupedBlocksChanged := make(map[uint][]uint)
		// Now we also look for extra blocks based on how much they have changed...
		bls := dtr.dt.dirtyLog.Collect(0, uint(dtr.dt.numBlocks))
		for _, b := range bls {
			groupedB := b >> groupByShift
			v, ok := groupedBlocksChanged[groupedB]
			if ok {
				groupedBlocksChanged[groupedB] = append(v, b)
			} else {
				groupedBlocksChanged[groupedB] = []uint{b}
			}
		}

		// Now sort by how much changed
		keys := make([]uint, 0, len(groupedBlocksChanged))
		for key := range groupedBlocksChanged {
			keys = append(keys, key)
		}

		// Sort the blocks by how many sub-blocks are dirty.
		sort.SliceStable(keys, func(i, j int) bool {
			return len(groupedBlocksChanged[keys[i]]) > len(groupedBlocksChanged[keys[j]])
		})

		// Now add them into grouped_blocks if we can...
		for {
			if len(groupedBlocks) == limit {
				break
			}
			if len(keys) == 0 {
				break
			}
			// Pick one out of grouped_blocks_changed, and try to add it
			k := keys[0]
			keys = keys[1:]

			if len(groupedBlocksChanged[k]) >= minChanged {
				// This may overwrite an existing entry which is max_age, but we'll have the same block + others here
				groupedBlocks[k] = groupedBlocksChanged[k]
			}
		}
	}

	// Clear out the tracking data here... It'll get added for tracking again on a readAt()
	rblocks := make([]uint, 0)
	for rb, blocks := range groupedBlocks {
		for _, b := range blocks {
			dtr.dt.trackingLock.Lock()
			dtr.dt.tracking.ClearBit(int(b))
			dtr.dt.dirtyLog.ClearBit(int(b))
			delete(dtr.dt.trackingTimes, b)
			dtr.dt.trackingLock.Unlock()
		}
		rblocks = append(rblocks, rb)
	}
	return rblocks
}

func (dtr *Remote) GetAllDirtyBlocks() *util.Bitfield {
	// Prevent any writes while we do the Sync()
	dtr.dt.writeLock.Lock()
	defer dtr.dt.writeLock.Unlock()

	info := dtr.dt.dirtyLog.Clone()

	// Remove the dirty blocks from tracking... (They will get added again when a Read is performed to migrate the data)
	dtr.dt.tracking.ClearBitsIf(info, 0, uint(dtr.dt.numBlocks))

	// Clear the dirty log.
	dtr.dt.dirtyLog.Clear()

	blocks := info.Collect(0, info.Length())
	dtr.dt.trackingLock.Lock()
	for _, b := range blocks {
		delete(dtr.dt.trackingTimes, b)
	}
	dtr.dt.trackingLock.Unlock()

	return info
}

func (dtr *Remote) Sync() *util.Bitfield {
	info := dtr.GetAllDirtyBlocks()
	return info
}

func (dt *DirtyTracker) localReadAt(buffer []byte, offset int64) (int, error) {
	return dt.prov.ReadAt(buffer, offset)
}

func (dt *DirtyTracker) localWriteAt(buffer []byte, offset int64) (int, error) {
	dt.writeLock.RLock()
	defer dt.writeLock.RUnlock()

	n, err := dt.prov.WriteAt(buffer, offset)

	if err == nil {
		end := uint64(offset + int64(len(buffer)))
		if end > dt.size {
			end = dt.size
		}

		bStart := uint(offset / int64(dt.blockSize))
		bEnd := uint((end-1)/uint64(dt.blockSize)) + 1

		// Update tracking times for last block write
		dt.trackingLock.Lock()
		dt.dirtyLog.SetBitsIf(dt.tracking, bStart, bEnd)
		now := time.Now()
		for b := bStart; b < bEnd; b++ {
			if dt.tracking.BitSet(int(b)) {
				dt.trackingTimes[b] = now
			}
		}
		dt.trackingLock.Unlock()
	}
	return n, err
}

func (dt *DirtyTracker) localFlush() error {
	return dt.prov.Flush()
}

func (dt *DirtyTracker) localSize() uint64 {
	return dt.prov.Size()
}

func (dt *DirtyTracker) remoteReadAt(buffer []byte, offset int64) (int, error) {

	// Start tracking dirty on the area we read.
	dt.trackArea(int64(len(buffer)), offset)
	// NB: A WriteAt could occur here, which would result in an incorrect dirty marking.
	// TODO: Do something to mitigate this without affecting performance.
	// Note though, that this is still preferable to tracking everything before it's been read for migration.
	n, err := dt.prov.ReadAt(buffer, offset)

	return n, err
}

func (dt *DirtyTracker) remoteWriteAt(buffer []byte, offset int64) (int, error) {
	return dt.prov.WriteAt(buffer, offset)
}

func (dt *DirtyTracker) remoteFlush() error {
	return dt.prov.Flush()
}

func (dt *DirtyTracker) remoteSize() uint64 {
	return dt.prov.Size()
}
