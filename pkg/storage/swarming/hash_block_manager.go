package swarming

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage"
)

var ErrBlockNotFound = errors.New("block not found")

// A manager keeps track of lots of different blocks, and their location(s)
type HashBlockManager struct {
	lock   sync.Mutex
	blocks map[string]*HashBlock

	metricGets         uint64
	metricGetsNotFound uint64
	metricAdds         uint64
}

type HashBlock struct {
	Size      int64
	Locations []HashBlockLocation
}

type HashBlockLocation interface {
	GetBytes(context.Context) ([]byte, error)
}

type HashBlockManagerMetrics struct {
	StoredHashes    uint64
	StoredLocations uint64
	Adds            uint64
	Gets            uint64
	GetsNotFound    uint64
}

func (hbm *HashBlockManager) GetMetrics() *HashBlockManagerMetrics {
	hbm.lock.Lock()
	defer hbm.lock.Unlock()
	totalLocations := 0
	for _, l := range hbm.blocks {
		totalLocations += len(l.Locations)
	}
	return &HashBlockManagerMetrics{
		StoredHashes:    uint64(len(hbm.blocks)),
		StoredLocations: uint64(totalLocations),
		Adds:            atomic.LoadUint64(&hbm.metricAdds),
		Gets:            atomic.LoadUint64(&hbm.metricGets),
		GetsNotFound:    atomic.LoadUint64(&hbm.metricGetsNotFound),
	}
}

func NewHashBlockManager() *HashBlockManager {
	return &HashBlockManager{
		blocks: make(map[string]*HashBlock),
	}
}

// Add a HashBlock to this store
func (hbm *HashBlockManager) Add(hash string, hb *HashBlock) {
	hbm.lock.Lock()
	defer hbm.lock.Unlock()

	atomic.AddUint64(&hbm.metricAdds, 1)

	h, ok := hbm.blocks[hash]
	if !ok {
		hbm.blocks[hash] = hb
	} else {
		h.Locations = append(h.Locations, hb.Locations...)
	}
}

// Get some data
func (hbm *HashBlockManager) Get(ctx context.Context, hash string) ([]byte, error) {
	hbm.lock.Lock()
	defer hbm.lock.Unlock()
	hb, ok := hbm.blocks[hash]
	if !ok {
		atomic.AddUint64(&hbm.metricGetsNotFound, 1)

		return nil, ErrBlockNotFound
	}

	atomic.AddUint64(&hbm.metricGets, 1)

	// TODO: We might want to do *some*? concurrently here - to try different locations until one is successful
	var allErrors error
	for _, l := range hb.Locations {
		data, err := l.GetBytes(ctx)
		if err == nil {
			return data, nil
		}
		allErrors = errors.Join(allErrors, err)
		// We might need a mechanism here to remove this location from the list. Or at least mark it.
	}
	return nil, allErrors
}

// Index a complete provider into this hashblockmanager
func (hbm *HashBlockManager) IndexStorage(p storage.Provider, blockSize int) error {
	size := p.Size()
	buffer := make([]byte, blockSize)
	for offset := uint64(0); offset < size; offset += uint64(blockSize) {
		// Read, hash, and create an entry.
		n, err := p.ReadAt(buffer, int64(offset))
		if err != nil {
			return err
		}
		hash := sha256.Sum256(buffer[:n])

		// Add the entry.
		hbm.Add(fmt.Sprintf("%x", hash), &HashBlock{
			Size: int64(n),
			Locations: []HashBlockLocation{
				&ProviderHBL{
					Offset:   int64(offset),
					Size:     int64(n),
					Provider: p,
				},
			},
		})
	}
	return nil
}
