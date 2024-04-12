package integrity

import (
	"crypto/sha256"

	"github.com/loopholelabs/silo/pkg/storage"
)

type IntegrityChecker struct {
	block_size int
	size       int64
	num_blocks int
	hashes     [][sha256.Size]byte
}

func NewIntegrityChecker(size int64, block_size int) *IntegrityChecker {
	num_blocks := (size + int64(block_size) - 1) / int64(block_size)
	return &IntegrityChecker{
		block_size: block_size,
		size:       size,
		num_blocks: int(num_blocks),
		hashes:     make([][sha256.Size]byte, num_blocks),
	}
}

/**
 * Update the hash for a particular block
 *
 */
func (i *IntegrityChecker) SetHash(block uint, hash [sha256.Size]byte) {
	i.hashes[block] = hash
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
		for d := 0; d < sha256.Size; d++ {
			if v[d] != i.hashes[b][d] {
				return false, nil
			}
		}
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
		// Calculate the hash and store it...
		i.hashes[b] = sha256.Sum256(block_buffer[:n])
	}
	return nil
}
