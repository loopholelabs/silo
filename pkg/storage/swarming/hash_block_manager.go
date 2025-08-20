package swarming

import (
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

// A manager keeps track of lots of different blocks, and their location(s)
type HashBlockManager struct {
	lock   sync.Mutex
	blocks map[string]*HashBlock
}

type HashBlock struct {
	Size      int64
	Locations []HashBlockLocation
}

type HashBlockLocation interface {
	GetBytes() ([]byte, error)
}

type ProviderHBL struct {
	Offset   int64
	Size     int64
	Provider storage.Provider
}

func (p *ProviderHBL) GetBytes() ([]byte, error) {
	buffer := make([]byte, p.Size)
	n, err := p.Provider.ReadAt(buffer, p.Offset)
	if err == nil {
		return buffer[:n], nil
	}
	return nil, err
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
	hbm.blocks[hash] = hb
}

// Index a complete provider into this hashblockmanager
func (hbm *HashBlockManager) IndexStorage(p storage.Provider, blockSize int) error {
	size := p.Size()
	buffer := make([]byte, blockSize)
	for offset := uint64(0); offset < size; offset++ {
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
