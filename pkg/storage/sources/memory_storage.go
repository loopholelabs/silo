package sources

import (
	"sync"
)

/**
 * Simple memory based storage provider
 *
 *
 */
type MemoryStorage struct {
	data []byte
	lock sync.RWMutex
}

func NewMemoryStorage(size int) *MemoryStorage {
	return &MemoryStorage{
		data: make([]byte, size),
	}
}

func (i *MemoryStorage) ReadAt(buffer []byte, offset int64) (int, error) {
	i.lock.RLock()
	n := copy(buffer, i.data[offset:])
	i.lock.RUnlock()
	return n, nil
}

func (i *MemoryStorage) WriteAt(buffer []byte, offset int64) (int, error) {
	i.lock.Lock()
	n := copy(i.data[offset:], buffer)
	i.lock.Unlock()
	return n, nil
}

func (i *MemoryStorage) Flush() error {
	return nil
}

func (i *MemoryStorage) Size() uint64 {
	return uint64(len(i.data))
}
