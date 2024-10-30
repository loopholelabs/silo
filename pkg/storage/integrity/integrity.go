package integrity

import (
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

type IntegrityChecker struct {
	blockSize int
	size      int64
	numBlocks int
	hashes    map[uint][sha256.Size]byte
	lock      sync.Mutex
}

func NewIntegrityChecker(size int64, blockSize int) *IntegrityChecker {
	numBlocks := (size + int64(blockSize) - 1) / int64(blockSize)
	return &IntegrityChecker{
		blockSize: blockSize,
		size:      size,
		numBlocks: int(numBlocks),
		hashes:    make(map[uint][sha256.Size]byte, numBlocks),
	}
}

/**
 * Update the hash for a particular block
 *
 */
func (i *IntegrityChecker) SetHash(block uint, hash [sha256.Size]byte) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.hashes[block] = hash
}

func (i *IntegrityChecker) SetHashes(hashes map[uint][sha256.Size]byte) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.hashes = hashes
}

// Grab a snapshot of hashes...
func (i *IntegrityChecker) GetHashes() map[uint][sha256.Size]byte {
	v := make(map[uint][sha256.Size]byte)
	i.lock.Lock()
	defer i.lock.Unlock()
	for b, h := range i.hashes {
		v[b] = h
	}
	return v
}

/**
 * Check that the given storage provider agrees with hashes.
 *
 * TODO: Calculate blocks concurrently
 */
func (i *IntegrityChecker) Check(prov storage.StorageProvider) (bool, error) {
	block_buffer := make([]byte, i.blockSize)
	for b := 0; b < i.numBlocks; b++ {
		n, err := prov.ReadAt(block_buffer, int64(b*i.blockSize))
		if err != nil {
			return false, err
		}
		// Calculate the hash...
		v := sha256.Sum256(block_buffer[:n])
		// Make sure it's same as the value we have...
		i.lock.Lock()
		hash, ok := i.hashes[uint(b)]
		if !ok {
			return false, fmt.Errorf("Hash not present for block %d", b)
		}
		for d := 0; d < sha256.Size; d++ {
			if v[d] != hash[d] {
				return false, nil
			}
		}
		i.lock.Unlock()

	}
	return true, nil
}

/**
 * Use the given storage provider to create block hashes
 *
 * TODO: Calculate blocks concurrently
 */
func (i *IntegrityChecker) Hash(prov storage.StorageProvider) error {
	blockBuffer := make([]byte, i.blockSize)
	for b := 0; b < i.numBlocks; b++ {
		n, err := prov.ReadAt(blockBuffer, int64(b*i.blockSize))
		if err != nil {
			return err
		}
		i.HashBlock(uint(b), blockBuffer[:n])
	}
	return nil
}

/**
 * Hash a block and store it in our map...
 *
 */
func (i *IntegrityChecker) HashBlock(block uint, data []byte) {
	h := sha256.Sum256(data)
	i.lock.Lock()
	defer i.lock.Unlock()
	i.hashes[block] = h
}
