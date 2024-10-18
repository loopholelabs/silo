package storage

import (
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type StorageProvider interface {
	io.ReaderAt
	io.WriterAt
	Size() uint64
	Flush() error
	Close() error
	CancelWrites(offset int64, length int64)
	UUID() []uuid.UUID
}

type StorageProviderWithLifecycleIfc interface {
	StorageProvider
	SetLifecycleState(LifecycleState)
	GetLifecycleState() LifecycleState
	AddLifecycleNotification(LifecycleState, LifecycleCallback)
}

// Try to change the lifecycle of a StorageProvider
func SetLifecycleState(s StorageProvider, state LifecycleState) bool {
	lcsp, ok := s.(StorageProviderWithLifecycleIfc)
	if ok {
		lcsp.SetLifecycleState(state)
	}
	return ok
}

// Try to get the lifecycle of a StorageProvider
func GetLifecycleState(s StorageProvider) (LifecycleState, bool) {
	lcsp, ok := s.(StorageProviderWithLifecycleIfc)
	if ok {
		return lcsp.GetLifecycleState(), true
	}
	return Lifecycle_none, false
}

// Try to add a lifecycle notification on a StorageProvider
func AddLifecycleNotification(s StorageProvider, state LifecycleState, callback LifecycleCallback) bool {
	lcsp, ok := s.(StorageProviderWithLifecycleIfc)
	if ok {
		lcsp.AddLifecycleNotification(state, callback)
	}
	return ok
}

/**
 * A StorageProvider can embed StorageProviderLifecycleState to support lifecycles
 *
 */
type LifecycleState int

const Lifecycle_none = LifecycleState(0)
const Lifecycle_active = LifecycleState(1)
const Lifecycle_migrating_to = LifecycleState(2)
const Lifecycle_migrating_from = LifecycleState(3)
const Lifecycle_closed = LifecycleState(4)

type LifecycleCallback func(from LifecycleState, to LifecycleState)

type StorageProviderLifecycleState struct {
	state_lock      sync.Mutex
	state           LifecycleState
	state_callbacks map[LifecycleState][]LifecycleCallback
}

// Set the current state, and notify any callbacks
func (spl *StorageProviderLifecycleState) SetLifecycleState(state LifecycleState) {
	spl.state_lock.Lock()
	defer spl.state_lock.Unlock()
	state_was := spl.state
	spl.state = state
	if spl.state_callbacks == nil {
		return
	}
	cbs, ok := spl.state_callbacks[state]
	if ok {
		for _, cb := range cbs {
			cb(state_was, state)
		}
	}
}

// Get the current state
func (spl *StorageProviderLifecycleState) GetLifecycleState() LifecycleState {
	spl.state_lock.Lock()
	defer spl.state_lock.Unlock()
	return spl.state
}

// Add a new callback for the given state.
func (spl *StorageProviderLifecycleState) AddLifecycleNotification(state LifecycleState, callback LifecycleCallback) {
	spl.state_lock.Lock()
	defer spl.state_lock.Unlock()
	if spl.state_callbacks == nil {
		spl.state_callbacks = make(map[LifecycleState][]LifecycleCallback)
	}
	_, ok := spl.state_callbacks[state]
	if ok {
		spl.state_callbacks[state] = append(spl.state_callbacks[state], callback)
	} else {
		spl.state_callbacks[state] = []LifecycleCallback{callback}
	}
}

type LockableStorageProvider interface {
	StorageProvider
	Lock()
	Unlock()
}

type TrackingStorageProvider interface {
	StorageProvider
	Sync() *util.Bitfield
}

type ExposedStorage interface {
	Init() error
	Shutdown() error
	Device() string
	SetProvider(prov StorageProvider)
}

type BlockOrder interface {
	AddAll()
	Add(block int)
	Remove(block int)
	GetNext() *BlockInfo
}

type BlockInfo struct {
	Block int
	Type  int
}

var BlockInfoFinish = &BlockInfo{Block: -1}

var BlockTypeAny = -1
var BlockTypeStandard = 0
var BlockTypeDirty = 1
var BlockTypePriority = 2

/**
 * Check if two storageProviders hold the same data.
 *
 */
func Equals(sp1 StorageProvider, sp2 StorageProvider, block_size int) (bool, error) {
	if sp1.Size() != sp2.Size() {
		return false, nil
	}

	size := int(sp1.Size())

	sourceBuff := make([]byte, block_size)
	destBuff := make([]byte, block_size)
	for i := 0; i < size; i += block_size {
		sourceBuff = sourceBuff[:cap(sourceBuff)]
		destBuff = destBuff[:cap(destBuff)]

		n, err := sp1.ReadAt(sourceBuff, int64(i))
		if err != nil {
			return false, err
		}
		sourceBuff = sourceBuff[:n]
		n, err = sp2.ReadAt(destBuff, int64(i))
		if err != nil {
			return false, err
		}
		destBuff = destBuff[:n]
		if len(sourceBuff) != len(destBuff) {
			return false, nil
		}
		for j := 0; j < n; j++ {
			if sourceBuff[j] != destBuff[j] {
				fmt.Printf("Equals: Block %d differs\n", i/block_size)
				return false, nil
			}
		}
	}

	return true, nil
}

/**
 * Map a function over blocks within the range.
 *
 */
func MapOverBlocks(offset int64, length int32, block_size int, f func(b int, complete bool)) {
	end := uint64(offset + int64(length))

	b_start := int(offset / int64(block_size))
	b_end := int((end-1)/uint64(block_size)) + 1
	for b := b_start; b < b_end; b++ {
		complete := true
		// If the first block is incomplete
		if offset > (int64(b_start) * int64(block_size)) {
			complete = false
		}
		// If the last block is incomplete
		if (end % uint64(block_size)) > 0 {
			complete = false
		}

		f(b, complete)
	}
}
