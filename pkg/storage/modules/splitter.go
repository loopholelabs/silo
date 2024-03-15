package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 * This splitter does the following...
 * There's a defaultProvider, which all reads and writes go to.
 * There's also a list of dynamic providers which can be added to/removed from.
 * Reads go through the dynamic providers, and if there's a hit, the data is returned.
 * Writes go to everything.
 * Reads to the defaultProvider get written back to all dynamic providers.
 */

type Splitter struct {
	providers_lock         sync.RWMutex
	providers              []storage.StorageProvider
	defaultProvider        storage.StorageProvider
	replicate_to_providers bool
}

func NewSplitter(prov storage.StorageProvider) *Splitter {
	return &Splitter{
		providers:              make([]storage.StorageProvider, 0),
		defaultProvider:        prov,
		replicate_to_providers: true,
	}
}

func (i *Splitter) AddProvider(prov storage.StorageProvider) {
	i.providers_lock.Lock()
	i.providers = append(i.providers, prov)
	i.providers_lock.Unlock()
}

func (i *Splitter) RemoveProvider(prov storage.StorageProvider) {
	i.providers_lock.Lock()
	new_providers := make([]storage.StorageProvider, 0)
	for _, p := range i.providers {
		if p != prov {
			new_providers = append(new_providers, p)
		}
	}
	i.providers = new_providers
	i.providers_lock.Unlock()
}

func (i *Splitter) ReadAt(buffer []byte, offset int64) (int, error) {
	i.providers_lock.RLock()
	for _, p := range i.providers {
		n, err := p.ReadAt(buffer, offset)
		if err == nil {
			return n, err
		}
	}
	i.providers_lock.RUnlock()

	n, err := i.defaultProvider.ReadAt(buffer, offset)
	if err != nil {
		return 0, err
	}
	if i.replicate_to_providers {
		// Do write backs to the providers
		i.providers_lock.RLock()
		for _, p := range i.providers {
			p.WriteAt(buffer[:n], offset)
		}
		i.providers_lock.RUnlock()
	}
	return n, nil
}

func (i *Splitter) WriteAt(buffer []byte, offset int64) (int, error) {
	i.providers_lock.RLock()
	for _, p := range i.providers {
		p.WriteAt(buffer, offset)
	}
	i.providers_lock.RUnlock()

	return i.defaultProvider.WriteAt(buffer, offset)
}

func (i *Splitter) Flush() error {
	i.providers_lock.RLock()
	for _, p := range i.providers {
		p.Flush()
		// FIXME: Should we do anything on an error here?
	}
	i.providers_lock.RUnlock()

	return i.defaultProvider.Flush()
}

func (i *Splitter) Size() uint64 {
	return i.defaultProvider.Size()
}

func (i *Splitter) Close() error {
	return i.defaultProvider.Close()
}
