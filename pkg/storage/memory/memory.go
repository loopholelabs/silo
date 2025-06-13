package memory

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

var ErrNotFound = errors.New("memory range not found")

type ProcessMemoryStorage struct {
	storage.ProviderWithEvents
	lock             sync.RWMutex
	pid              int
	size             uint64
	memRanges        []MapMemoryRange
	memf             *os.File
	unrequiredBlocks func() []uint
}

// NewProcessMemoryStorage creates a new provider
func NewProcessMemoryStorage(pid int, device string, unrequiredBlocks func() []uint) (*ProcessMemoryStorage, error) {
	pm := NewProcessMemory(pid)
	memRanges, err := pm.GetMemoryRange(device)
	if err != nil {
		return nil, err
	}

	size := uint64(0)
	for _, r := range memRanges {
		size += (r.End - r.Start)
	}

	memf, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pid), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	return &ProcessMemoryStorage{
		pid:              pid,
		memRanges:        memRanges,
		size:             size,
		memf:             memf,
		unrequiredBlocks: unrequiredBlocks,
	}, nil
}

// Respond to EventTypeCowGetBlocks
func (i *ProcessMemoryStorage) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	if eventType == storage.EventTypeCowGetBlocks {
		return []storage.EventReturnData{
			i.unrequiredBlocks(),
		}
	}

	return i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
}

// Find the memory range we need for a specified offset
func (i *ProcessMemoryStorage) findMemoryRange(start uint64) *MapMemoryRange {
	for _, r := range i.memRanges {
		if start >= r.Offset && start < (r.Offset+r.End-r.Start) {
			return &r
		}
	}
	return nil
}

func (i *ProcessMemoryStorage) ReadAt(buffer []byte, offset int64) (int, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()

	fmt.Printf("#Memory ReadAt %x len %x\n", offset, len(buffer))

	bufferOffset := int64(0)
	for {
		if bufferOffset == int64(len(buffer)) {
			break // We're done
		}
		r := i.findMemoryRange(uint64(offset + bufferOffset))
		if r == nil {
			return len(buffer), nil // lie for now
			// return 0, ErrNotFound
		}
		memOffset := uint64(offset+bufferOffset) - r.Offset
		canRead := (r.End - r.Start) - memOffset               // How much could we read from the range?
		canWrite := uint64(len(buffer)) - uint64(bufferOffset) // How much could we write to the buffer
		if canWrite < canRead {
			canRead = canWrite
		}

		n, err := i.memf.ReadAt(buffer[bufferOffset:uint64(bufferOffset)+canRead], int64(r.Start+memOffset))
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		bufferOffset += int64(n)
	}

	return len(buffer), nil
}

func (i *ProcessMemoryStorage) WriteAt(buffer []byte, offset int64) (int, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	bufferOffset := int64(0)
	for {
		if bufferOffset == int64(len(buffer)) {
			break // We're done
		}
		r := i.findMemoryRange(uint64(offset + bufferOffset))
		if r == nil {
			return 0, ErrNotFound
		}
		memOffset := uint64(offset+bufferOffset) - r.Offset
		canRead := (r.End - r.Start) - memOffset               // How much could we read from the range?
		canWrite := uint64(len(buffer)) - uint64(bufferOffset) // How much could we write to the buffer
		if canWrite < canRead {
			canRead = canWrite
		}

		n, err := i.memf.WriteAt(buffer[bufferOffset:uint64(bufferOffset)+canRead], int64(r.Start+memOffset))
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		bufferOffset += int64(n)
	}

	return len(buffer), nil
}

func (i *ProcessMemoryStorage) Flush() error {
	return nil
}

func (i *ProcessMemoryStorage) Size() uint64 {
	return i.size
}

func (i *ProcessMemoryStorage) Close() error {
	return i.memf.Close()
}

func (i *ProcessMemoryStorage) CancelWrites(_ int64, _ int64) {}
