package memory

import (
	"errors"
)

type Tracker struct {
	Pid           int
	lastMaps      *MapsFile
	processMemory *ProcessMemory
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
		processMemory: NewProcessMemory(pid),
		lockProcess:   lockProcess,
		unlockProcess: unlockProcess,
	}
}

// Callbacks for Update()
type Callbacks struct {
	AddPages    func(addr []uint64)
	RemovePages func(addrs []uint64)
	UpdatePages func(data []byte, addr uint64) error
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

	// Now look for dirty pages, and copy out the memory
	for _, e := range maps.Entries {
		ranges, err := mt.processMemory.ReadSoftDirtyMemoryRangeListFilter(e.AddrStart, e.AddrEnd,
			func() error { return nil },
			func() error { return nil },
			func(_ uint64) bool { return true })
		if err != nil {
			pmErrors = errors.Join(pmErrors, err)
		} else {
			allRanges = append(allRanges, ranges...)
		}
	}
	err = mt.processMemory.ClearSoftDirty()
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
		cb.AddPages(addedPages)
	}
	removedPages := maps.AddedPages(mt.lastMaps)
	if len(removedPages) > 0 {
		cb.RemovePages(removedPages)
	}
	mt.lastMaps = maps

	// Now copy the memory, and report to callbacks
	_, err = mt.processMemory.CopyMemoryRangesFunc(allRanges, cb.UpdatePages)
	if err != nil {
		pmErrors = errors.Join(pmErrors, err)
	}

	return pmErrors
}
