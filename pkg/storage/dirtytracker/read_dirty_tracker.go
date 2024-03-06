package dirtytracker

import (
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

/**
 * This can track writes so that we can get a list of dirty areas after some period.
 * Tracking is enabled for a block when a Read is performed on that block.
 * A call to sync() grabs the list of dirty blocks, and resets those to not be monitored.
 */

type DirtyTracker struct {
	prov      storage.StorageProvider
	size      uint64
	blockSize int
	numBlocks int
	dirtyLog  *util.Bitfield
	tracking  *util.Bitfield
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
	info := i.dt.dirtyLog.Clone()

	// Remove these blocks from tracking... (They will get added again when a Read is performed to migrate the data)
	i.dt.tracking.ClearBitsIf(info, 0, uint(i.dt.numBlocks))

	i.dt.dirtyLog.Clear()
	return info
}

func (i *DirtyTracker) localReadAt(buffer []byte, offset int64) (int, error) {
	return i.prov.ReadAt(buffer, offset)
}

func (i *DirtyTracker) localWriteAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.blockSize))
	b_end := uint((end-1)/uint64(i.blockSize)) + 1

	i.dirtyLog.SetBitsIf(i.tracking, b_start, b_end)

	return i.prov.WriteAt(buffer, offset)
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
	// FIXME: If a write happens here, then we'll get a dirtyLog for it where we don't need one. We should couple the SetBits to the ReadAt.
	return i.prov.ReadAt(buffer, offset)
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
