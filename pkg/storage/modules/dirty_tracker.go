package modules

import (
	"sort"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

type WriteInfo struct {
	Offset int64
	Length int
}

type FilterDirtyTracker struct {
	prov       storage.StorageProvider
	dirty_lock sync.Mutex
	dirty_log  []*WriteInfo
	tracking   bool
}

func NewFilterDirtyTracker(prov storage.StorageProvider) *FilterDirtyTracker {
	return &FilterDirtyTracker{
		prov:     prov,
		tracking: false,
	}
}

func CleanupWriteInfo(wis []*WriteInfo) []*WriteInfo {
	// Sort it first...
	sort.Slice(wis, func(a, b int) bool {
		return wis[a].Offset < wis[b].Offset
	})

	// Now look for continuous regions...
	cleaned_log := make([]*WriteInfo, 0)
	for _, wi := range wis {
		if len(cleaned_log) == 0 {
			cleaned_log = append(cleaned_log, wi)
		} else {
			last := cleaned_log[len(cleaned_log)-1]
			// Check if it's a dupe offset...
			if wi.Offset == last.Offset {
				// If so, then pick the largest length.
				if wi.Length > last.Length {
					last.Length = wi.Length
				}
			} else {
				// Check if it's right after the last one...
				if wi.Offset == (last.Offset + int64(last.Length)) {
					last.Length += wi.Length // Just add it on to the previous
				} else {
					// Create a new entry
					cleaned_log = append(cleaned_log, wi)
				}
			}
		}
	}
	return cleaned_log
}

func (i *FilterDirtyTracker) Track() {
	i.dirty_lock.Lock()
	i.tracking = true
	i.dirty_lock.Unlock()
}

func (i *FilterDirtyTracker) Sync() []*WriteInfo {
	i.dirty_lock.Lock()
	info := i.dirty_log
	i.dirty_log = make([]*WriteInfo, 0)
	i.tracking = false
	i.dirty_lock.Unlock()
	return info
}

func (i *FilterDirtyTracker) ReadAt(buffer []byte, offset int64) (int, error) {
	return i.prov.ReadAt(buffer, offset)
}

func (i *FilterDirtyTracker) WriteAt(buffer []byte, offset int64) (int, error) {
	// Log writes here...
	i.dirty_lock.Lock()
	if i.tracking {
		i.dirty_log = append(i.dirty_log, &WriteInfo{
			Offset: offset,
			Length: len(buffer),
		})
	}
	i.dirty_lock.Unlock()
	return i.prov.WriteAt(buffer, offset)
}

func (i *FilterDirtyTracker) Flush() error {
	return i.prov.Flush()
}

func (i *FilterDirtyTracker) Size() uint64 {
	return i.prov.Size()
}
