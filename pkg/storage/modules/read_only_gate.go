package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 *
 */

type ReadOnlyGate struct {
	storage.ProviderWithEvents
	prov   storage.Provider
	lock   *sync.Cond
	locked bool
}

// Relay events to embedded StorageProvider
func (i *ReadOnlyGate) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewReadOnlyGate(prov storage.Provider) *ReadOnlyGate {
	return &ReadOnlyGate{
		prov:   prov,
		lock:   sync.NewCond(&sync.Mutex{}),
		locked: false,
	}
}

func (i *ReadOnlyGate) ReadAt(p []byte, off int64) (n int, err error) {
	return i.prov.ReadAt(p, off)
}

func (i *ReadOnlyGate) WriteAt(p []byte, off int64) (n int, err error) {
	i.lock.L.Lock()
	if i.locked {
		i.lock.Wait()
	}
	i.lock.L.Unlock()

	return i.prov.WriteAt(p, off)
}

func (i *ReadOnlyGate) Flush() error {
	return i.prov.Flush()
}

func (i *ReadOnlyGate) Size() uint64 {
	return i.prov.Size()
}

func (i *ReadOnlyGate) Close() error {
	return i.prov.Close()
}

func (i *ReadOnlyGate) CancelWrites(_ int64, _ int64) {
	// TODO: Implement
}

func (i *ReadOnlyGate) Unlock() {
	i.lock.L.Lock()
	i.locked = false
	i.lock.Broadcast()
	i.lock.L.Unlock()
}

func (i *ReadOnlyGate) Lock() {
	i.lock.L.Lock()
	i.locked = true
	i.lock.L.Unlock()
}
