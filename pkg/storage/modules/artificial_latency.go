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
	lock                   sync.RWMutex
	prov                   storage.StorageProvider
	latency_read           time.Duration
	latency_write          time.Duration
	latency_read_per_byte  time.Duration
	latency_write_per_byte time.Duration
}

func NewArtificialLatency(prov storage.StorageProvider, latencyRead time.Duration, latencyReadPerByte time.Duration, latencyWrite time.Duration, latencyWritePerByte time.Duration) *ArtificialLatency {
	return &ArtificialLatency{
		prov:                   prov,
		latency_read:           latencyRead,
		latency_write:          latencyWrite,
		latency_read_per_byte:  latencyReadPerByte,
		latency_write_per_byte: latencyWritePerByte,
	}
}

func (i *ArtificialLatency) ReadAt(buffer []byte, offset int64) (int, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	if i.latency_read != 0 {
		time.Sleep(i.latency_read)
	}
	if i.latency_read_per_byte != 0 {
		time.Sleep(i.latency_read_per_byte * time.Duration(len(buffer)))
	}
	return i.prov.ReadAt(buffer, offset)
}

func (i *ArtificialLatency) WriteAt(buffer []byte, offset int64) (int, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.latency_write != 0 {
		time.Sleep(i.latency_write)
	}
	if i.latency_write_per_byte != 0 {
		time.Sleep(i.latency_write_per_byte * time.Duration(len(buffer)))
	}
	return i.prov.WriteAt(buffer, offset)
}

func (i *ArtificialLatency) Flush() error {
	return i.prov.Flush()
}

func (i *ArtificialLatency) Size() uint64 {
	return i.prov.Size()
}

func (i *ArtificialLatency) Close() error {
	return i.prov.Close()
}
