package memory

import (
	"errors"
)

type MemoryTracker struct {
	Pid           int
	lastMaps      *MapsFile
	processMemory *ProcessMemory
}

func NewMemoryTracker(pid int) *MemoryTracker {
	return &MemoryTracker{
		Pid: pid,
		lastMaps: &MapsFile{
			Pid:     pid,
			Entries: []*MapsEntry{},
		},
		processMemory: NewProcessMemory(pid),
	}
}

// Callbacks for Update()
type Callbacks struct {
	AddPages    func(addr []uint64)
	RemovePages func(addrs []uint64)
	UpdatePages func(data []byte, addr uint64) error
}

// Update finds what has changed in memory
func (mt *MemoryTracker) Update(cb *Callbacks) error {
	maps, err := GetMaps(mt.Pid)
	if err != nil {
		return err
	}

	addedPages := mt.lastMaps.AddedPages(maps)
	if len(addedPages) > 0 {
		cb.AddPages(addedPages)
	}
	removedPages := maps.AddedPages(mt.lastMaps)
	if len(removedPages) > 0 {
		cb.RemovePages(removedPages)
	}
	mt.lastMaps = maps

	var pmErrors error
	// Now look for dirty pages, and copy out the memory
	for _, e := range maps.Entries {
		ranges, err := mt.processMemory.ReadSoftDirtyMemoryRangeListFilter(e.AddrStart, e.AddrEnd,
			func() error { return nil },
			func() error { return nil },
			func(_ uint64) bool { return true })
		if err != nil {
			pmErrors = errors.Join(pmErrors, err)
		} else {
			_, err = mt.processMemory.CopyMemoryRangesFunc(ranges, cb.UpdatePages)
			if err != nil {
				pmErrors = errors.Join(pmErrors, err)
			}
		}
	}

	clearErr := mt.processMemory.ClearSoftDirty()
	return errors.Join(pmErrors, clearErr)
}
