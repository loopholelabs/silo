package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type CopyOnWrite struct {
	storage.ProviderWithEvents
	source     storage.Provider
	cache      storage.Provider
	exists     *util.Bitfield
	size       uint64
	blockSize  int
	CloseFn    func()
	lock       sync.Mutex
	wg         sync.WaitGroup
	sharedBase bool
}

// Relay events to embedded StorageProvider
func (i *CopyOnWrite) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	if i.sharedBase {
		// Something is asking for a Base provider. Respond with the source provider.
		if eventType == storage.EventType("base.get") {
			return []storage.EventReturnData{
				i.source,
			}
		}
	}

	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	data = append(data, storage.SendSiloEvent(i.cache, eventType, eventData)...)
	return append(data, storage.SendSiloEvent(i.source, eventType, eventData)...)
}

func NewCopyOnWrite(source storage.Provider, cache storage.Provider, blockSize int) *CopyOnWrite {
	numBlocks := (source.Size() + uint64(blockSize) - 1) / uint64(blockSize)
	return &CopyOnWrite{
		source:     source,
		cache:      cache,
		exists:     util.NewBitfield(int(numBlocks)),
		size:       source.Size(),
		blockSize:  blockSize,
		CloseFn:    func() {},
		sharedBase: true,
	}
}

func NewCopyOnWriteHiddenBase(source storage.Provider, cache storage.Provider, blockSize int) *CopyOnWrite {
	numBlocks := (source.Size() + uint64(blockSize) - 1) / uint64(blockSize)
	return &CopyOnWrite{
		source:     source,
		cache:      cache,
		exists:     util.NewBitfield(int(numBlocks)),
		size:       source.Size(),
		blockSize:  blockSize,
		CloseFn:    func() {},
		sharedBase: false,
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

	bufferEnd := int64(len(buffer))
	if offset+int64(len(buffer)) > int64(i.size) {
		// Get rid of any extra data that we can't store...
		bufferEnd = int64(i.size) - offset
	}

	end := uint64(offset + bufferEnd)
	if end > i.size {
		end = i.size
	}

	bStart := uint(offset / int64(i.blockSize))
	bEnd := uint((end-1)/uint64(i.blockSize)) + 1

	// Special case where all the data is ALL in the cache
	if i.exists.BitsSet(bStart, bEnd) {
		return i.cache.ReadAt(buffer, offset)
	}

	blocks := bEnd - bStart
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	for b := bStart; b < bEnd; b++ {
		count := 0
		blockOffset := int64(b) * int64(i.blockSize)
		var err error
		if blockOffset >= offset {
			// Partial read at the end
			if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
				if i.exists.BitSet(int(b)) {
					count, err = i.cache.ReadAt(buffer[blockOffset-offset:bufferEnd], blockOffset)
				} else {
					blockBuffer := make([]byte, i.blockSize)
					// Read existing data
					_, err = i.source.ReadAt(blockBuffer, blockOffset)
					if err == nil {
						count = copy(buffer[blockOffset-offset:bufferEnd], blockBuffer)
					}
				}
			} else {
				// Complete block reads in the middle
				s := blockOffset - offset
				e := s + int64(i.blockSize)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				if i.exists.BitSet(int(b)) {
					_, err = i.cache.ReadAt(buffer[s:e], blockOffset)
				} else {
					_, err = i.source.ReadAt(buffer[s:e], blockOffset)
				}
				count = i.blockSize
			}
		} else {
			// Partial read at the start
			if i.exists.BitSet(int(b)) {
				plen := i.blockSize - int(offset-blockOffset)
				if plen > int(bufferEnd) {
					plen = int(bufferEnd)
				}
				count, err = i.cache.ReadAt(buffer[:plen], offset)
			} else {
				blockBuffer := make([]byte, i.blockSize)
				_, err = i.source.ReadAt(blockBuffer, blockOffset)
				if err == nil {
					count = copy(buffer[:bufferEnd], blockBuffer[offset-blockOffset:])
				}
			}
		}
		errs <- err
		counts <- count
	}

	// Wait for completion, Check for errors and return...
	count := 0
	for b := bStart; b < bEnd; b++ {
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

	bufferEnd := int64(len(buffer))
	if offset+int64(len(buffer)) > int64(i.size) {
		// Get rid of any extra data that we can't store...
		bufferEnd = int64(i.size) - offset
	}

	end := uint64(offset + bufferEnd)
	if end > i.size {
		end = i.size
	}

	bStart := uint(offset / int64(i.blockSize))
	bEnd := uint((end-1)/uint64(i.blockSize)) + 1

	// Special case where all the data is in the cache
	if i.exists.BitsSet(bStart, bEnd) {
		n, err := i.cache.WriteAt(buffer, offset)
		return n, err
	}

	blocks := bEnd - bStart
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	// Now we have a series of non-overlapping writes
	for b := bStart; b < bEnd; b++ {

		blockOffset := int64(b) * int64(i.blockSize)
		var err error
		count := 0
		if blockOffset >= offset {
			// Partial write at the end
			if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
				if i.exists.BitSet(int(b)) {
					count, err = i.cache.WriteAt(buffer[blockOffset-offset:bufferEnd], blockOffset)
				} else {
					blockBuffer := make([]byte, i.blockSize)
					// Read existing data
					_, err = i.source.ReadAt(blockBuffer, blockOffset)
					if err == nil {
						// Merge in data
						count = copy(blockBuffer, buffer[blockOffset-offset:bufferEnd])
						// Write back to cache
						_, err = i.cache.WriteAt(blockBuffer, blockOffset)
						i.exists.SetBit(int(b))
					}
				}
			} else {
				// Complete block writes in the middle
				s := blockOffset - offset
				e := s + int64(i.blockSize)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				_, err = i.cache.WriteAt(buffer[s:e], blockOffset)
				i.exists.SetBit(int(b))
				count = i.blockSize
			}
		} else {
			// Partial write at the start
			if i.exists.BitSet(int(b)) {

				plen := i.blockSize - int(offset-blockOffset)
				if plen > int(bufferEnd) {
					plen = int(bufferEnd)
				}
				count, err = i.cache.WriteAt(buffer[:plen], offset)
			} else {
				blockBuffer := make([]byte, i.blockSize)
				_, err = i.source.ReadAt(blockBuffer, blockOffset)
				if err == nil {
					// Merge in data
					count = copy(blockBuffer[offset-blockOffset:], buffer[:bufferEnd])
					_, err = i.cache.WriteAt(blockBuffer, blockOffset)
					i.exists.SetBit(int(b))
				}
			}
		}
		errs <- err
		counts <- count
	}

	// Wait for completion, Check for errors and return...
	count := 0
	for b := bStart; b < bEnd; b++ {
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
	i.CloseFn()
	return nil
}

func (i *CopyOnWrite) CancelWrites(offset int64, length int64) {
	i.cache.CancelWrites(offset, length)
}
