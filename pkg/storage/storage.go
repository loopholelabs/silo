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

type StorageProviderWithEventsIfc interface {
	StorageProvider
	SendEvent(EventType, EventData) []EventReturnData
	AddEventNotification(EventType, EventCallback)
}

// Try to send an event for a StorageProvider
func SendEvent(s StorageProvider, event_type EventType, event_data EventData) []EventReturnData {
	lcsp, ok := s.(StorageProviderWithEventsIfc)
	if ok {
		return lcsp.SendEvent(event_type, event_data)
	}
	return nil
}

// Try to add a lifecycle notification on a StorageProvider
func AddEventNotification(s StorageProvider, state EventType, callback EventCallback) bool {
	lcsp, ok := s.(StorageProviderWithEventsIfc)
	if ok {
		lcsp.AddEventNotification(state, callback)
	}
	return ok
}

/**
 * A StorageProvider can embed StorageProviderWithEvents to support events
 *
 */
type EventType string
type EventData interface{}
type EventReturnData interface{}

type EventCallback func(event EventType, data EventData) EventReturnData

type StorageProviderWithEvents struct {
	lock      sync.Mutex
	callbacks map[EventType][]EventCallback
}

// Set the current state, and notify any callbacks
func (spl *StorageProviderWithEvents) SendEvent(event_type EventType, event_data EventData) []EventReturnData {
	spl.lock.Lock()
	defer spl.lock.Unlock()
	if spl.callbacks == nil {
		return nil
	}
	cbs, ok := spl.callbacks[event_type]
	if ok {
		rets := make([]EventReturnData, 0)
		for _, cb := range cbs {
			rets = append(rets, cb(event_type, event_data))
		}
		return rets
	}
	return nil
}

// Add a new callback for the given state.
func (spl *StorageProviderWithEvents) AddEventNotification(event_type EventType, callback EventCallback) {
	spl.lock.Lock()
	defer spl.lock.Unlock()
	if spl.callbacks == nil {
		spl.callbacks = make(map[EventType][]EventCallback)
	}
	_, ok := spl.callbacks[event_type]
	if ok {
		spl.callbacks[event_type] = append(spl.callbacks[event_type], callback)
	} else {
		spl.callbacks[event_type] = []EventCallback{callback}
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
