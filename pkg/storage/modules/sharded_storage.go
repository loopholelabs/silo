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

func NewShardedStorage(size int, blocksize int, creator func(index int, size int) storage.StorageProvider) *ShardedStorage {
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
		bms.blocks = append(bms.blocks, creator(i, d))
		left -= d
	}
	return bms
}

func (i *ShardedStorage) ReadAt(buffer []byte, offset int64) (int, error) {
	errs := make(chan error, 2+(len(buffer)/i.blocksize))

	left := len(buffer)
	ptr := 0
	num_reads := 0
	for {
		if left == 0 || offset >= int64(i.size) {
			break
		}
		s := offset / int64(i.blocksize)
		si := offset - (s * int64(i.blocksize)) // Index into block

		e := (ptr + i.blocksize - int(si))
		if e > len(buffer) {
			e = len(buffer)
		}
		count := e - ptr

		// Do reads concurrently
		go func(prov storage.StorageProvider, dest []byte, off int64) {
			_, err := prov.ReadAt(dest, off)
			errs <- err
		}(i.blocks[s], buffer[ptr:e], si)

		num_reads++

		offset += int64(count)
		ptr += count
		left -= count
	}

	for i := 0; i < num_reads; i++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
	}

	return ptr, nil
}

func (i *ShardedStorage) WriteAt(buffer []byte, offset int64) (int, error) {
	errs := make(chan error, 2+(len(buffer)/i.blocksize))

	left := len(buffer)
	ptr := 0
	num_reads := 0
	for {
		if left == 0 || offset >= int64(i.size) {
			break
		}
		s := offset / int64(i.blocksize)
		si := offset - (s * int64(i.blocksize))

		e := (ptr + i.blocksize - int(si))
		if e > len(buffer) {
			e = len(buffer)
		}
		count := e - ptr

		// Do writes concurrently
		go func(prov storage.StorageProvider, dest []byte, off int64) {
			_, err := prov.WriteAt(dest, off)
			errs <- err
		}(i.blocks[s], buffer[ptr:e], si)

		num_reads++

		offset += int64(count)
		ptr += count
		left -= count
	}

	for i := 0; i < num_reads; i++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
	}

	return ptr, nil

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
