package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 *
 */

type BlockSplitter struct {
	prov       storage.StorageProvider
	block_size int
	size       uint64
}

func NewBlockSplitter(prov storage.StorageProvider, block_size int) *BlockSplitter {
	return &BlockSplitter{
		prov:       prov,
		block_size: block_size,
		size:       prov.Size(),
	}
}

func (i *BlockSplitter) ReadAt(buffer []byte, offset int64) (n int, err error) {
	// Split the read up into blocks, and concurrenty perform the reads...
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	blocks := b_end - b_start
	errs := make(chan error, blocks)

	for b := b_start; b < b_end; b++ {
		go func(block_no uint) {
			block_offset := int64(block_no) * int64(i.block_size)
			var err error
			if block_offset > offset {
				// Partial read at the end
				if len(buffer[block_offset-offset:]) < i.block_size {
					block_buffer := make([]byte, i.block_size)
					_, err = i.prov.ReadAt(block_buffer, block_offset)
					copy(buffer[block_offset-offset:], block_buffer)
				} else {
					// Complete read in the middle
					s := block_offset - offset
					e := s + int64(i.block_size)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = i.prov.ReadAt(buffer[s:e], block_offset)
				}
			} else {
				// Partial read at the start
				block_buffer := make([]byte, i.block_size)
				_, err = i.prov.ReadAt(block_buffer, block_offset)
				copy(buffer, block_buffer[offset-block_offset:])
			}
			errs <- err
		}(b)
	}

	// Wait for completion, Check for errors and return...
	for b := b_start; b < b_end; b++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
	}

	return len(buffer), nil
}

func (i *BlockSplitter) WriteAt(p []byte, off int64) (n int, err error) {
	// Split the write up into blocks, and concurrenty perform the writes...

	// TODO

	return i.prov.WriteAt(p, off)
}

func (i *BlockSplitter) Flush() error {
	return i.prov.Flush()
}

func (i *BlockSplitter) Size() uint64 {
	return i.prov.Size()
}