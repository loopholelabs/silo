package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 * Simple sparse cache with LRU
 *
 */
type SparseCache struct {
	prov storage.StorageProvider
	// TODO: Cache store here
}

func NewSparseCache(prov storage.StorageProvider) *SparseCache {
	return &SparseCache{
		prov: prov,
	}
}

func (i *SparseCache) ReadAt(buffer []byte, offset int64) (int, error) {
	// TODO: Try to read the data from our cache
	return 0, nil
}

func (i *SparseCache) WriteAt(buffer []byte, offset int64) (int, error) {
	// TODO: Write the data to our cache
	return 0, nil
}

func (i *SparseCache) Flush() error {
	return nil
}

func (i *SparseCache) Size() uint64 {
	return i.prov.Size()
}
