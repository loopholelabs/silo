package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type CopyOnWrite struct {
	storage.StorageProviderWithEvents
	source     storage.StorageProvider
	cache      storage.StorageProvider
	exists     *util.Bitfield
	size       uint64
	block_size int
	Close_fn   func()
	lock       sync.Mutex
	wg         sync.WaitGroup
}

// Relay events to embedded StorageProvider
func (i *CopyOnWrite) SendSiloEvent(event_type storage.EventType, event_data storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendSiloEvent(event_type, event_data)
	data = append(data, storage.SendSiloEvent(i.cache, event_type, event_data)...)
	return append(data, storage.SendSiloEvent(i.source, event_type, event_data)...)
}

func NewCopyOnWrite(source storage.StorageProvider, cache storage.StorageProvider, blockSize int) *CopyOnWrite {
	numBlocks := (source.Size() + uint64(blockSize) - 1) / uint64(blockSize)
	return &CopyOnWrite{
		source:     source,
		cache:      cache,
		exists:     util.NewBitfield(int(numBlocks)),
		size:       source.Size(),
		block_size: blockSize,
		Close_fn:   func() {},
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
	i.wg.Add(1)
	defer i.wg.Done()

	buffer_end := int64(len(buffer))
	if offset+int64(len(buffer)) > int64(i.size) {
		// Get rid of any extra data that we can't store...
		buffer_end = int64(i.size) - offset
	}

	end := uint64(offset + buffer_end)
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	// Special case where all the data is ALL in the cache
	if i.exists.BitsSet(b_start, b_end) {
		return i.cache.ReadAt(buffer, offset)
	}

	blocks := b_end - b_start
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	for b := b_start; b < b_end; b++ {
		count := 0
		block_offset := int64(b) * int64(i.block_size)
		var err error
		if block_offset >= offset {
			// Partial read at the end
			if len(buffer[block_offset-offset:buffer_end]) < i.block_size {
				if i.exists.BitSet(int(b)) {
					count, err = i.cache.ReadAt(buffer[block_offset-offset:buffer_end], block_offset)
				} else {
					block_buffer := make([]byte, i.block_size)
					// Read existing data
					_, err = i.source.ReadAt(block_buffer, block_offset)
					if err == nil {
						count = copy(buffer[block_offset-offset:buffer_end], block_buffer)
					}
				}
			} else {
				// Complete block reads in the middle
				s := block_offset - offset
				e := s + int64(i.block_size)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				if i.exists.BitSet(int(b)) {
					_, err = i.cache.ReadAt(buffer[s:e], block_offset)
				} else {
					_, err = i.source.ReadAt(buffer[s:e], block_offset)
				}
				count = i.block_size
			}
		} else {
			// Partial read at the start
			if i.exists.BitSet(int(b)) {
				plen := i.block_size - int(offset-block_offset)
				if plen > int(buffer_end) {
					plen = int(buffer_end)
				}
				count, err = i.cache.ReadAt(buffer[:plen], offset)
			} else {
				block_buffer := make([]byte, i.block_size)
				_, err = i.source.ReadAt(block_buffer, block_offset)
				if err == nil {
					count = copy(buffer[:buffer_end], block_buffer[offset-block_offset:])
				}
			}
		}
		errs <- err
		counts <- count
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
	i.wg.Add(1)
	defer i.wg.Done()

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

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	// Special case where all the data is in the cache
	if i.exists.BitsSet(b_start, b_end) {
		n, err := i.cache.WriteAt(buffer, offset)
		return n, err
	}

	blocks := b_end - b_start
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	// Now we have a series of non-overlapping writes
	for b := b_start; b < b_end; b++ {

		block_offset := int64(b) * int64(i.block_size)
		var err error
		count := 0
		if block_offset >= offset {
			// Partial write at the end
			if len(buffer[block_offset-offset:buffer_end]) < i.block_size {
				if i.exists.BitSet(int(b)) {
					count, err = i.cache.WriteAt(buffer[block_offset-offset:buffer_end], block_offset)
				} else {
					block_buffer := make([]byte, i.block_size)
					// Read existing data
					_, err = i.source.ReadAt(block_buffer, block_offset)
					if err == nil {
						// Merge in data
						count = copy(block_buffer, buffer[block_offset-offset:buffer_end])
						// Write back to cache
						_, err = i.cache.WriteAt(block_buffer, block_offset)
						i.exists.SetBit(int(b))
					}
				}
			} else {
				// Complete block writes in the middle
				s := block_offset - offset
				e := s + int64(i.block_size)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				_, err = i.cache.WriteAt(buffer[s:e], block_offset)
				i.exists.SetBit(int(b))
				count = i.block_size
			}
		} else {
			// Partial write at the start
			if i.exists.BitSet(int(b)) {

				plen := i.block_size - int(offset-block_offset)
				if plen > int(buffer_end) {
					plen = int(buffer_end)
				}
				count, err = i.cache.WriteAt(buffer[:plen], offset)
			} else {
				block_buffer := make([]byte, i.block_size)
				_, err = i.source.ReadAt(block_buffer, block_offset)
				if err == nil {
					// Merge in data
					count = copy(block_buffer[offset-block_offset:], buffer[:buffer_end])
					_, err = i.cache.WriteAt(block_buffer, block_offset)
					i.exists.SetBit(int(b))
				}
			}
		}
		errs <- err
		counts <- count
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
	i.wg.Wait() // Wait for any pending reads/writes to complete
	i.cache.Close()
	i.source.Close()
	i.Close_fn()
	return nil
}

func (i *CopyOnWrite) CancelWrites(offset int64, length int64) {
	i.cache.CancelWrites(offset, length)
}
