package modules

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 * Simple cache StorageProvider
 * This tracks which areas of the source we have, and will only allow reads for areas we have.
 *
 */

var Err_Cache_Miss = errors.New("Cache Miss")

type Cache struct {
	prov                storage.StorageProvider
	exists              storage.StorageProvider
	metric_hits         uint64
	metric_hits_bytes   uint64
	metric_misses       uint64
	metric_misses_bytes uint64
}

func NewCache(prov storage.StorageProvider, exists storage.StorageProvider) *Cache {
	return &Cache{
		prov:          prov,
		exists:        exists,
		metric_hits:   0,
		metric_misses: 0,
	}
}

func (i *Cache) ShowStats(prefix string) {
	hits := atomic.LoadUint64(&i.metric_hits)
	hits_bytes := atomic.LoadUint64(&i.metric_hits_bytes)
	misses := atomic.LoadUint64(&i.metric_misses)
	misses_bytes := atomic.LoadUint64(&i.metric_misses_bytes)

	// TODO: Optimize this properly...

	// Find out how much data we have
	buffer := make([]byte, 4096)
	size := i.prov.Size()
	offset := int64(0)
	exist_count := int64(0)
	for {
		if offset == int64(size) {
			break // All done
		}
		n, _ := i.exists.ReadAt(buffer, offset)
		// FIXME: Handle error
		for _, v := range buffer[:n] {
			exist_count += int64(v)
		}
		offset += int64(n)
	}

	perc := float64(exist_count) * 100 / float64(size)
	fmt.Printf("%s: (%d/%d %.2f%%) hits %d (%d bytes) misses %d (%d bytes)\n", prefix, exist_count, size, perc, hits, hits_bytes, misses, misses_bytes)
}

// TODO: Optimize these two properly...
func (i *Cache) containsRange(offset int64, length int) bool {
	e := make([]byte, length)
	i.exists.ReadAt(e, offset)
	// FIXME: Deal with error here?
	for _, v := range e {
		if v == 0 {
			return false
		}
	}
	return true
}

func (i *Cache) acceptRange(offset int64, length int) {
	e := make([]byte, length)
	for x := 0; x < length; x++ {
		e[x] = 1
	}
	i.exists.WriteAt(e, offset)
	// FIXME: Deal with error here?
}

func (i *Cache) ReadAt(buffer []byte, offset int64) (int, error) {
	if i.containsRange(offset, len(buffer)) {
		n, err := i.prov.ReadAt(buffer, offset)
		if err == nil {
			atomic.AddUint64(&i.metric_hits, 1)
			atomic.AddUint64(&i.metric_hits_bytes, uint64(n))
		}
		return n, err
	} else {
		atomic.AddUint64(&i.metric_misses, 1)
		atomic.AddUint64(&i.metric_misses_bytes, uint64(len(buffer)))
	}
	return 0, Err_Cache_Miss
}

func (i *Cache) WriteAt(buffer []byte, offset int64) (int, error) {
	// Mark the range as existing
	n, err := i.prov.WriteAt(buffer, offset)
	i.acceptRange(offset, n)
	return n, err
}

func (i *Cache) Flush() error {
	return i.prov.Flush()
}

func (i *Cache) Size() uint64 {
	return i.prov.Size()
}

func (i *Cache) Close() error {
	return i.prov.Close()
}
