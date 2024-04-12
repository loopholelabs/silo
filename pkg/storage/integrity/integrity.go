package integrity

import (
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

type IntegrityChecker struct {
	block_size int
	size       int64
	num_blocks int
	hashes     map[uint][sha256.Size]byte
	lock       sync.Mutex
}

func NewIntegrityChecker(size int64, block_size int) *IntegrityChecker {
	num_blocks := (size + int64(block_size) - 1) / int64(block_size)
	return &IntegrityChecker{
		block_size: block_size,
		size:       size,
		num_blocks: int(num_blocks),
		hashes:     make(map[uint][sha256.Size]byte, num_blocks),
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
	block_buffer := make([]byte, i.block_size)
	for b := 0; b < i.num_blocks; b++ {
		n, err := prov.ReadAt(block_buffer, int64(b*i.block_size))
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
	block_buffer := make([]byte, i.block_size)
	for b := 0; b < i.num_blocks; b++ {
		n, err := prov.ReadAt(block_buffer, int64(b*i.block_size))
		if err != nil {
			return err
		}
		i.HashBlock(uint(b), block_buffer[:n])
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
