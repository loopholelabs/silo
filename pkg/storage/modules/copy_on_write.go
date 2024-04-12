package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type CopyOnWrite struct {
	source     storage.StorageProvider
	cache      storage.StorageProvider
	exists     *util.Bitfield
	size       uint64
	blockSize  int
	blockLocks []sync.Mutex
	CloseFunc  func()
	lock       sync.Mutex
}

func NewCopyOnWrite(source storage.StorageProvider, cache storage.StorageProvider, blockSize int) *CopyOnWrite {
	numBlocks := (source.Size() + uint64(blockSize) - 1) / uint64(blockSize)
	return &CopyOnWrite{
		source:     source,
		cache:      cache,
		exists:     util.NewBitfield(int(numBlocks)),
		size:       source.Size(),
		blockSize:  blockSize,
		blockLocks: make([]sync.Mutex, numBlocks),
		CloseFunc:  func() {},
	}
}

func (i *CopyOnWrite) SetBlockExists(blocks []uint) {
	for _, b := range blocks {
		i.exists.SetBit(int(b))
	}
}

func (i *CopyOnWrite) GetBlockExists() []uint {
	return i.exists.Collect(0, i.exists.Length())
}

func (i *CopyOnWrite) ReadAt(buffer []byte, offset int64) (int, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	buffer_end := int64(len(buffer))
	if offset+int64(len(buffer)) > int64(i.size) {
		// Get rid of any extra data that we can't store...
		buffer_end = int64(i.size) - offset
	}

	end := uint64(offset + buffer_end)
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.blockSize))
	b_end := uint((end-1)/uint64(i.blockSize)) + 1

	// Special case where all the data is ALL in the cache
	if i.exists.BitsSet(b_start, b_end) {
		return i.cache.ReadAt(buffer, offset)
	}

	blocks := b_end - b_start
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	for b := b_start; b < b_end; b++ {
		go func(block_no uint) {
			count := 0
			block_offset := int64(block_no) * int64(i.blockSize)
			var err error
			if block_offset >= offset {
				// Partial read at the end
				if len(buffer[block_offset-offset:buffer_end]) < i.blockSize {
					if i.exists.BitSet(int(block_no)) {
						count, err = i.cache.ReadAt(buffer[block_offset-offset:buffer_end], block_offset)
					} else {
						i.blockLocks[block_no].Lock()
						block_buffer := make([]byte, i.blockSize)
						// Read existing data
						_, err = i.source.ReadAt(block_buffer, block_offset)
						if err == nil {
							count = copy(buffer[block_offset-offset:buffer_end], block_buffer)
						}
						i.blockLocks[block_no].Unlock()
					}
				} else {
					// Complete block reads in the middle
					s := block_offset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					if i.exists.BitSet(int(block_no)) {
						_, err = i.cache.ReadAt(buffer[s:e], block_offset)
					} else {
						i.blockLocks[block_no].Lock()
						_, err = i.source.ReadAt(buffer[s:e], block_offset)
						i.blockLocks[block_no].Unlock()
					}
					count = i.blockSize
				}
			} else {
				// Partial read at the start
				if i.exists.BitSet(int(block_no)) {
					plen := i.blockSize - int(offset-block_offset)
					if plen > int(buffer_end) {
						plen = int(buffer_end)
					}
					count, err = i.cache.ReadAt(buffer[:plen], offset)
				} else {
					i.blockLocks[block_no].Lock()
					block_buffer := make([]byte, i.blockSize)
					_, err = i.source.ReadAt(block_buffer, block_offset)
					if err == nil {
						count = copy(buffer[:buffer_end], block_buffer[offset-block_offset:])
					}
					i.blockLocks[block_no].Unlock()
				}
			}
			errs <- err
			counts <- count
		}(b)
	}

	// Wait for completion, Check for errors and return...
	count := 0
	for b := b_start; b < b_end; b++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
		c := <-counts
		count += c
	}

	return count, nil
}

func (i *CopyOnWrite) WriteAt(buffer []byte, offset int64) (int, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	buffer_end := int64(len(buffer))
	if offset+int64(len(buffer)) > int64(i.size) {
		// Get rid of any extra data that we can't store...
		buffer_end = int64(i.size) - offset
	}

	end := uint64(offset + buffer_end)
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.blockSize))
	b_end := uint((end-1)/uint64(i.blockSize)) + 1

	// Special case where all the data is in the cache
	if i.exists.BitsSet(b_start, b_end) {
		return i.cache.WriteAt(buffer, offset)
	}

	blocks := b_end - b_start
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	for b := b_start; b < b_end; b++ {
		go func(block_no uint) {
			block_offset := int64(block_no) * int64(i.blockSize)
			var err error
			count := 0
			if block_offset >= offset {
				// Partial write at the end
				if len(buffer[block_offset-offset:buffer_end]) < i.blockSize {
					if i.exists.BitSet(int(block_no)) {
						count, err = i.cache.WriteAt(buffer[block_offset-offset:buffer_end], block_offset)
					} else {
						i.blockLocks[block_no].Lock()
						block_buffer := make([]byte, i.blockSize)
						// Read existing data
						_, err = i.source.ReadAt(block_buffer, block_offset)
						if err == nil {
							// Merge in data
							count = copy(block_buffer, buffer[block_offset-offset:buffer_end])
							// Write back to cache
							_, err = i.cache.WriteAt(block_buffer, block_offset)
							i.exists.SetBit(int(block_no))
						}
						i.blockLocks[block_no].Unlock()
					}
				} else {
					// Complete block writes in the middle
					s := block_offset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					i.blockLocks[block_no].Lock()
					_, err = i.cache.WriteAt(buffer[s:e], block_offset)
					i.exists.SetBit(int(block_no))
					i.blockLocks[block_no].Unlock()
					count = i.blockSize
				}
			} else {
				// Partial write at the start
				if i.exists.BitSet(int(block_no)) {
					plen := i.blockSize - int(offset-block_offset)
					if plen > int(buffer_end) {
						plen = int(buffer_end)
					}
					count, err = i.cache.WriteAt(buffer[:plen], offset)
				} else {
					i.blockLocks[block_no].Lock()
					block_buffer := make([]byte, i.blockSize)
					_, err = i.source.ReadAt(block_buffer, block_offset)
					if err == nil {
						// Merge in data
						count = copy(block_buffer[offset-block_offset:], buffer[:buffer_end])
						_, err = i.cache.WriteAt(block_buffer, block_offset)
						i.exists.SetBit(int(block_no))
					}
					i.blockLocks[block_no].Unlock()
				}
			}
			errs <- err
			counts <- count
		}(b)
	}

	// Wait for completion, Check for errors and return...
	count := 0
	for b := b_start; b < b_end; b++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
		c := <-counts
		count += c
	}

	return count, nil
}

func (i *CopyOnWrite) Flush() error {
	return nil
}

func (i *CopyOnWrite) Size() uint64 {
	return i.source.Size()
}

func (i *CopyOnWrite) Close() error {
	i.cache.Close()
	i.source.Close()
	i.CloseFunc()
	return nil
}
