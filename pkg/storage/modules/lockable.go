package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 * This adds the ability to lock a storageprovider, and wait until it's unlocked.
 *
 */

type Lockable struct {
	storage.StorageProviderWithEvents
	prov   storage.StorageProvider
	lock   *sync.Cond
	locked bool
}

// Relay events to embedded StorageProvider
func (i *Lockable) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewLockable(prov storage.StorageProvider) *Lockable {
	return &Lockable{
		prov:   prov,
		lock:   sync.NewCond(&sync.Mutex{}),
		locked: false,
	}
}

func (i *Lockable) ReadAt(p []byte, off int64) (n int, err error) {
	i.lock.L.Lock()
	if i.locked {
		i.lock.Wait()
	}
	i.lock.L.Unlock()

	return i.prov.ReadAt(p, off)
}

func (i *Lockable) WriteAt(p []byte, off int64) (n int, err error) {
	i.lock.L.Lock()
	if i.locked {
		i.lock.Wait()
	}
	i.lock.L.Unlock()

	return i.prov.WriteAt(p, off)
}

func (i *Lockable) Flush() error {
	return i.prov.Flush()
}

func (i *Lockable) Size() uint64 {
	return i.prov.Size()
}

func (i *Lockable) Close() error {
	return i.prov.Close()
}

func (i *Lockable) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}

func (i *Lockable) Unlock() {
	i.lock.L.Lock()
	defer i.lock.L.Unlock()
	i.locked = false
	i.lock.Broadcast()
}

func (i *Lockable) Lock() {
	i.lock.L.Lock()
	defer i.lock.L.Unlock()
	i.locked = true
}

func (i *Lockable) IsLocked() bool {
	i.lock.L.Lock()
	defer i.lock.L.Unlock()
	return i.locked
}
