package modules

/**
 * ShardedStorage is a StorageProvider which shards across multiple StorageProviders.
 * Using this, we can for example do concurrent writes across shards, or we can mix different providers.
 *
 */

import (
	"fmt"

	"github.com/loopholelabs/silo/pkg/storage"
)

type ShardedStorage struct {
	blocks     []storage.StorageProvider
	block_size int
	size       int
}

func NewShardedStorage(size int, blocksize int, creator func(index int, size int) (storage.StorageProvider, error)) (*ShardedStorage, error) {
	if blocksize == 0 {
		return nil, fmt.Errorf("Invalid block size of 0")
	}
	bms := &ShardedStorage{
		blocks:     make([]storage.StorageProvider, 0),
		block_size: blocksize,
		size:       size,
	}
	left := size
	n := 0
	for i := 0; i < size; i += blocksize {
		d := blocksize
		if left < blocksize {
			d = left
		}
		b, err := creator(n, d)
		if err != nil {
			return nil, err
		}
		bms.blocks = append(bms.blocks, b)
		left -= d
		n++
	}
	return bms, nil
}

func (i *ShardedStorage) ReadAt(buffer []byte, offset int64) (int, error) {
	errs := make(chan error, 2+(len(buffer)/i.block_size))
	counts := make(chan int, 2+(len(buffer)/i.block_size))

	left := len(buffer)
	ptr := 0
	num_reads := 0
	for {
		if left == 0 || offset >= int64(i.size) {
			break
		}
		s := offset / int64(i.block_size)
		si := offset - (s * int64(i.block_size)) // Index into block

		e := (ptr + i.block_size - int(si))
		if e > len(buffer) {
			e = len(buffer)
		}
		count := e - ptr

		// Do reads concurrently
		go func(prov storage.StorageProvider, dest []byte, off int64) {
			n, err := prov.ReadAt(dest, off)
			errs <- err
			counts <- n
		}(i.blocks[s], buffer[ptr:e], si)

		num_reads++

		offset += int64(count)
		ptr += count
		left -= count
	}

	count := 0
	for i := 0; i < num_reads; i++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
		c := <-counts
		count += c
	}

	return count, nil
}

func (i *ShardedStorage) WriteAt(buffer []byte, offset int64) (int, error) {
	errs := make(chan error, 2+(len(buffer)/i.block_size))
	counts := make(chan int, 2+(len(buffer)/i.block_size))

	left := len(buffer)
	ptr := 0
	num_reads := 0
	for {
		if left == 0 || offset >= int64(i.size) {
			break
		}
		s := offset / int64(i.block_size)
		si := offset - (s * int64(i.block_size))

		e := (ptr + i.block_size - int(si))
		if e > len(buffer) {
			e = len(buffer)
		}
		count := e - ptr

		// Do writes concurrently
		go func(prov storage.StorageProvider, dest []byte, off int64) {
			n, err := prov.WriteAt(dest, off)
			errs <- err
			counts <- n
		}(i.blocks[s], buffer[ptr:e], si)

		num_reads++

		offset += int64(count)
		ptr += count
		left -= count
	}

	count := 0
	for i := 0; i < num_reads; i++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
		c := <-counts
		count += c
	}

	return count, nil

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

func (i *ShardedStorage) Close() error {
	var err error
	for _, b := range i.blocks {
		e := b.Close()
		if e != nil {
			err = e
		}
	}
	return err
}
