package storage

import (
	"io"

	"github.com/loopholelabs/silo/pkg/storage/util"
)

type StorageError int

const StorageError_SUCCESS = StorageError(0)
const StorageError_ERROR = StorageError(1)

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
	Handle(prov StorageProvider) error
	WaitReady() error
	Shutdown() error
}

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
