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
	storage.StorageProviderWithEvents
	lock                sync.RWMutex
	prov                storage.StorageProvider
	latencyRead         time.Duration
	latencyWrite        time.Duration
	latencyReadPerByte  time.Duration
	latencyWritePerByte time.Duration
}

// Relay events to embedded StorageProvider
func (i *ArtificialLatency) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewArtificialLatency(prov storage.StorageProvider, latencyRead time.Duration, latencyReadPerByte time.Duration, latencyWrite time.Duration, latencyWritePerByte time.Duration) *ArtificialLatency {
	return &ArtificialLatency{
		prov:                prov,
		latencyRead:         latencyRead,
		latencyWrite:        latencyWrite,
		latencyReadPerByte:  latencyReadPerByte,
		latencyWritePerByte: latencyWritePerByte,
	}
}

func (i *ArtificialLatency) ReadAt(buffer []byte, offset int64) (int, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	if i.latencyRead != 0 {
		time.Sleep(i.latencyRead)
	}
	if i.latencyReadPerByte != 0 {
		time.Sleep(i.latencyReadPerByte * time.Duration(len(buffer)))
	}
	return i.prov.ReadAt(buffer, offset)
}

func (i *ArtificialLatency) WriteAt(buffer []byte, offset int64) (int, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.latencyWrite != 0 {
		time.Sleep(i.latencyWrite)
	}
	if i.latencyWritePerByte != 0 {
		time.Sleep(i.latencyWritePerByte * time.Duration(len(buffer)))
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

func (i *ArtificialLatency) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
