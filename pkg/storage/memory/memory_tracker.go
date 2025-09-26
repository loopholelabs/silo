package memory

import (
	"errors"
	"slices"
)

type Tracker struct {
	Pid           int
	allPids       []int
	lastMaps      map[int]*MapsFile
	lockProcess   func() error
	unlockProcess func() error
}

func NewMemoryTracker(pid int, lockProcess func() error, unlockProcess func() error) *Tracker {
	return &Tracker{
		Pid:           pid,
		allPids:       []int{},
		lastMaps:      make(map[int]*MapsFile),
		lockProcess:   lockProcess,
		unlockProcess: unlockProcess,
	}
}

// Callbacks for Update()
type Callbacks struct {
	AddPID      func(pid int)
	RemovePID   func(pid int)
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

	allPids, err := GetPids(mt.Pid)
	if err != nil {
		errUnlock := mt.unlockProcess()
		return errors.Join(err, errUnlock)
	}
	allPids = append(allPids, mt.Pid)

	for _, pid := range allPids {
		if !slices.Contains(mt.allPids, pid) {
			cb.AddPID(pid)
			mt.lastMaps[pid] = &MapsFile{
				Pid:     pid,
				Entries: []*MapsEntry{},
			}
		}
	}
	for _, pid := range mt.allPids {
		if !slices.Contains(allPids, pid) {
			cb.RemovePID(pid)
			delete(mt.lastMaps, pid)
		}
	}
	mt.allPids = allPids

	// Now go through all PIDS
	pidRanges := make(map[int][]Range)
	pidMaps := make(map[int]*MapsFile)
	var pmErrors error

	for _, pid := range allPids {
		maps, err := GetMaps(pid)
		if err != nil {
			errUnlock := mt.unlockProcess()
			return errors.Join(err, errUnlock)
		}

		pidMaps[pid] = maps

		allRanges := make([]Range, 0)
		processMemory := NewProcessMemory(pid)

		// Now look for dirty pages
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
		pidRanges[pid] = allRanges
	}

	err = mt.unlockProcess()
	if err != nil {
		return err
	}

	for pid, maps := range pidMaps {
		// Report changes to the callbacks
		addedPages := mt.lastMaps[pid].AddedPages(maps)
		if len(addedPages) > 0 {
			cb.AddPages(pid, addedPages)
		}
		removedPages := maps.AddedPages(mt.lastMaps[pid])
		if len(removedPages) > 0 {
			cb.RemovePages(pid, removedPages)
		}
		mt.lastMaps[pid] = maps
	}

	for pid, allRanges := range pidRanges {
		// Now copy the memory, and report to callbacks
		processMemory := NewProcessMemory(pid)
		_, err = processMemory.CopyMemoryRangesFunc(allRanges, func(b []byte, u uint64) error {
			return cb.UpdatePages(pid, b, u)
		})
	}

	if err != nil {
		pmErrors = errors.Join(pmErrors, err)
	}

	return pmErrors
}
