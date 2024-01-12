package modules

/**
 * ShardedStorage is a StorageProvider which shards across multiple StorageProviders.
 * Using this, we can for example do concurrent writes across shards, or we can mix different providers.
 *
 */

import (
	"github.com/loopholelabs/silo/pkg/storage"
)

type ShardedStorage struct {
	blocks    []storage.StorageProvider
	blocksize int
	size      int
}

func NewShardedStorage(size int, blocksize int, creator func(size int) storage.StorageProvider) *ShardedStorage {
	bms := &ShardedStorage{
		blocks:    make([]storage.StorageProvider, 0),
		blocksize: blocksize,
		size:      size,
	}
	left := size
	for i := 0; i < size; i += blocksize {
		d := blocksize
		if left < blocksize {
			d = left
		}
		bms.blocks = append(bms.blocks, creator(d))
		left -= d
	}
	return bms
}

func (i *ShardedStorage) ReadAt(buffer []byte, offset int64) (int, error) {
	left := len(buffer)
	ptr := 0
	for {
		if left == 0 {
			break
		}
		s := offset / int64(i.blocksize)
		si := offset - (s * int64(i.blocksize)) // Index into block
		n, err := i.blocks[s].ReadAt(buffer[ptr:], si)
		if err != nil {
			return 0, err
		}
		offset += int64(n)
		ptr += n
		left -= n
	}
	return len(buffer), nil
}

func (i *ShardedStorage) WriteAt(buffer []byte, offset int64) (int, error) {
	left := len(buffer)
	ptr := 0
	for {
		if left == 0 {
			break
		}
		s := offset / int64(i.blocksize)
		si := offset - (s * int64(i.blocksize))
		n, err := i.blocks[s].WriteAt(buffer[ptr:], si)
		if err != nil {
			return 0, err
		}
		offset += int64(n)
		ptr += n
		left -= n
	}
	return len(buffer), nil

}

func (i *ShardedStorage) Flush() error {
	for _, s := range i.blocks {
		err := s.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *ShardedStorage) Size() uint64 {
	return uint64(i.size)
}
