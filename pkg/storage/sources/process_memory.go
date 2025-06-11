package sources

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

type ProcessMemoryStorage struct {
	storage.ProviderWithEvents
	lock sync.RWMutex
	pid  int
	size uint64
}

func NewProcessMemoryStorage(pid int, device string) *ProcessMemoryStorage {
	return &ProcessMemoryStorage{
		pid: pid,
	}
}

func (i *ProcessMemoryStorage) ReadAt(buffer []byte, offset int64) (int, error) {
	i.lock.RLock()
	i.lock.RUnlock()
	return len(buffer), nil
}

func (i *ProcessMemoryStorage) WriteAt(buffer []byte, offset int64) (int, error) {
	i.lock.Lock()
	i.lock.Unlock()
	return len(buffer), nil
}

func (i *ProcessMemoryStorage) Flush() error {
	return nil
}

func (i *ProcessMemoryStorage) Size() uint64 {
	return i.size
}

func (i *ProcessMemoryStorage) Close() error {
	return nil
}

func (i *ProcessMemoryStorage) CancelWrites(_ int64, _ int64) {}
