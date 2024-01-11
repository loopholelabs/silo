package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

type Lockable struct {
	prov   storage.StorageProvider
	lock   *sync.Cond
	locked bool
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

func (i *Lockable) Unlock() {
	i.lock.L.Lock()
	i.locked = false
	i.lock.Broadcast()
	i.lock.L.Unlock()
}

func (i *Lockable) Lock() {
	i.lock.L.Lock()
	i.locked = true
	i.lock.L.Unlock()
}
