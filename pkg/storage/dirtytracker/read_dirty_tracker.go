package dirtytracker

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

/**
 * This can track writes so that we can get a list of dirty areas after some period.
 * Tracking is enabled for a block when a Read is performed on the remote for that block.
 * A call to sync() grabs the list of dirty blocks, and resets those to not be monitored.
 */

type DirtyTracker struct {
	prov      storage.StorageProvider
	size      uint64
	blockSize int
	numBlocks int
	dirtyLog  *util.Bitfield
	tracking  *util.Bitfield
	writeLock sync.RWMutex
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

func NewDirtyTracker(prov storage.StorageProvider, blockSize int) (*DirtyTrackerLocal, *DirtyTrackerRemote) {
	size := int(prov.Size())
	numBlocks := (size + blockSize - 1) / blockSize
	dt := &DirtyTracker{
		size:      prov.Size(),
		blockSize: blockSize,
		numBlocks: numBlocks,
		prov:      prov,
		tracking:  util.NewBitfield(numBlocks),
		dirtyLog:  util.NewBitfield(numBlocks),
	}
	return &DirtyTrackerLocal{dt: dt}, &DirtyTrackerRemote{dt: dt}
}

func (i *DirtyTrackerRemote) Sync() *util.Bitfield {
	// Prevent any writes while we do the Sync()
	i.dt.writeLock.Lock()
	defer i.dt.writeLock.Unlock()

	info := i.dt.dirtyLog.Clone()

	// Remove the dirty blocks from tracking... (They will get added again when a Read is performed to migrate the data)
	i.dt.tracking.ClearBitsIf(info, 0, uint(i.dt.numBlocks))

	// Clear the dirty log.
	i.dt.dirtyLog.Clear()
	return info
}

func (i *DirtyTracker) localReadAt(buffer []byte, offset int64) (int, error) {
	return i.prov.ReadAt(buffer, offset)
}

func (i *DirtyTracker) localWriteAt(buffer []byte, offset int64) (int, error) {
	i.writeLock.RLock()
	defer i.writeLock.RUnlock()

	n, err := i.prov.WriteAt(buffer, offset)

	if err == nil {
		end := uint64(offset + int64(len(buffer)))
		if end > i.size {
			end = i.size
		}

		b_start := uint(offset / int64(i.blockSize))
		b_end := uint((end-1)/uint64(i.blockSize)) + 1

		i.dirtyLog.SetBitsIf(i.tracking, b_start, b_end)
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
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.blockSize))
	b_end := uint((end-1)/uint64(i.blockSize)) + 1

	// Enable tracking for this area
	i.tracking.SetBits(b_start, b_end)
	// NB: A WriteAt could occur here, which would result in an incorrect dirty marking.
	// TODO: Do something to mitigate this without affecting performance.
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
