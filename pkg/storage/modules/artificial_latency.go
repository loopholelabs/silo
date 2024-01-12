package modules

import (
	"sync"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 * Simple artificial latency for tests etc
 * Adds a RWMutex for this, so that the added latency is within a lock
 *
 */
type ArtificialLatency struct {
	lock         sync.RWMutex
	prov         storage.StorageProvider
	latencyRead  time.Duration
	latencyWrite time.Duration
}

func NewArtificialLatency(prov storage.StorageProvider, latencyRead time.Duration, latencyWrite time.Duration) *ArtificialLatency {
	return &ArtificialLatency{
		prov:         prov,
		latencyRead:  latencyRead,
		latencyWrite: latencyWrite,
	}
}

func (i *ArtificialLatency) ReadAt(buffer []byte, offset int64) (int, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	if i.latencyRead != 0 {
		time.Sleep(i.latencyRead)
	}
	return i.prov.ReadAt(buffer, offset)
}

func (i *ArtificialLatency) WriteAt(buffer []byte, offset int64) (int, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.latencyWrite != 0 {
		time.Sleep(i.latencyWrite)
	}
	return i.prov.WriteAt(buffer, offset)
}

func (i *ArtificialLatency) Flush() error {
	return i.prov.Flush()
}

func (i *ArtificialLatency) Size() uint64 {
	return i.prov.Size()
}
