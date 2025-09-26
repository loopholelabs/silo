package memory

import (
	"errors"
)

type Tracker struct {
	Pid           int
	lastMaps      *MapsFile
	lockProcess   func() error
	unlockProcess func() error
}

func NewMemoryTracker(pid int, lockProcess func() error, unlockProcess func() error) *Tracker {
	return &Tracker{
		Pid: pid,
		lastMaps: &MapsFile{
			Pid:     pid,
			Entries: []*MapsEntry{},
		},
		lockProcess:   lockProcess,
		unlockProcess: unlockProcess,
	}
}

// Callbacks for Update()
type Callbacks struct {
	AddPages    func(pid int, addr []uint64)
	RemovePages func(pid int, addrs []uint64)
	UpdatePages func(pid int, data []byte, addr uint64) error
}

// Update finds what has changed in memory
func (mt *Tracker) Update(cb *Callbacks) error {
	err := mt.lockProcess()
	if err != nil {
		return err
	}
	maps, err := GetMaps(mt.Pid)
	if err != nil {
		errUnlock := mt.unlockProcess()
		return errors.Join(err, errUnlock)
	}

	var pmErrors error
	allRanges := make([]Range, 0)

	processMemory := NewProcessMemory(mt.Pid)

	// Now look for dirty pages, and copy out the memory
	for _, e := range maps.Entries {
		ranges, err := processMemory.ReadSoftDirtyMemoryRangeListFilter(e.AddrStart, e.AddrEnd,
			func() error { return nil },
			func() error { return nil },
			func(_ uint64) bool { return true })
		if err != nil {
			pmErrors = errors.Join(pmErrors, err)
		} else {
			allRanges = append(allRanges, ranges...)
		}
	}
	err = processMemory.ClearSoftDirty()
	if err != nil {
		errUnlock := mt.unlockProcess()
		return errors.Join(err, errUnlock)
	}

	err = mt.unlockProcess()
	if err != nil {
		return err
	}

	// Report changes to the callbacks
	addedPages := mt.lastMaps.AddedPages(maps)
	if len(addedPages) > 0 {
		cb.AddPages(mt.Pid, addedPages)
	}
	removedPages := maps.AddedPages(mt.lastMaps)
	if len(removedPages) > 0 {
		cb.RemovePages(mt.Pid, removedPages)
	}
	mt.lastMaps = maps

	// Now copy the memory, and report to callbacks
	_, err = processMemory.CopyMemoryRangesFunc(allRanges, func(b []byte, u uint64) error {
		return cb.UpdatePages(mt.Pid, b, u)
	})

	if err != nil {
		pmErrors = errors.Join(pmErrors, err)
	}

	return pmErrors
}
