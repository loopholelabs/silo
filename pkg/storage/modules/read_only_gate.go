package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 *
 */

type ReadOnlyGate struct {
	prov   storage.StorageProvider
	lock   *sync.Cond
	locked bool
}

func NewReadOnlyGate(prov storage.StorageProvider) *Lockable {
	return &Lockable{
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
