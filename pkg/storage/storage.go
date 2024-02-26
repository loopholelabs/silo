package storage

import (
	"io"

	"github.com/loopholelabs/silo/pkg/storage/util"
)

type StorageProvider interface {
	io.ReaderAt
	io.WriterAt
	Size() uint64
	Flush() error
}

type LockableStorageProvider interface {
	StorageProvider
	Lock()
	Unlock()
}

type TrackingStorageProvider interface {
	StorageProvider
	Sync() *util.Bitfield
}

type ExposedStorage interface {
	Handle() error
	WaitReady() error
	Shutdown() error
}

type BlockOrder interface {
	AddAll()
	Add(block int)
	Remove(block int)
	GetNext() *BlockInfo
}

type BlockInfo struct {
	Block int
	Type  int
}

var BlockInfoFinish = &BlockInfo{Block: -1}

var BlockTypeAny = -1
var BlockTypeStandard = 0
var BlockTypeDirty = 1
var BlockTypePriority = 2

/**
 * Check if two storageProviders hold the same data.
 *
 */
func Equals(sp1 StorageProvider, sp2 StorageProvider, block_size int) (bool, error) {
	if sp1.Size() != sp2.Size() {
		return false, nil
	}

	size := int(sp1.Size())

	sourceBuff := make([]byte, block_size)
	destBuff := make([]byte, block_size)
	for i := 0; i < size; i += block_size {
		n, err := sp1.ReadAt(sourceBuff, int64(i))
		if n != block_size || err != nil {
			return false, err
		}
		n, err = sp2.ReadAt(destBuff, int64(i))
		if n != block_size || err != nil {
			return false, err
		}
		for j := 0; j < block_size; j++ {
			if sourceBuff[j] != destBuff[j] {
				return false, nil
			}
		}
	}

	return true, nil
}

/**
 * Map a function over blocks within the range.
 *
 */
func MapOverBlocks(offset int64, length int32, block_size int, f func(b int, complete bool)) {
	end := uint64(offset + int64(length))

	b_start := int(offset / int64(block_size))
	b_end := int((end-1)/uint64(block_size)) + 1
	for b := b_start; b < b_end; b++ {
		complete := true
		// If the first block is incomplete
		if offset > (int64(b_start) * int64(block_size)) {
			complete = false
		}
		// If the last block is incomplete
		if (end % uint64(block_size)) > 0 {
			complete = false
		}

		f(b, complete)
	}
}
