package modules

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type BlockCache struct {
	storage.ProviderWithEvents
	prov       storage.Provider
	size       uint64
	blockLocks []*sync.RWMutex
	blocks     map[uint]*blockInfo
	blocksLock sync.Mutex
	blockSize  int
	numBlocks  int
	maxBlocks  int
	exists     *util.Bitfield

	metricReadHits    uint64
	metricReadMisses  uint64
	metricWriteHits   uint64
	metricWriteMisses uint64
	metricFlushBlocks uint64
}

type blockInfo struct {
	data       []byte
	lastAccess time.Time
}

// Relay events to embedded StorageProvider
func (i *BlockCache) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewBlockCache(prov storage.Provider, blockSize int, maxBlocks int) *BlockCache {
	numBlocks := (int(prov.Size()) + blockSize - 1) / blockSize
	locks := make([]*sync.RWMutex, numBlocks)
	for t := 0; t < numBlocks; t++ {
		locks[t] = &sync.RWMutex{}
	}

	return &BlockCache{
		prov:       prov,
		size:       prov.Size(),
		blocks:     make(map[uint]*blockInfo),
		maxBlocks:  maxBlocks,
		blockSize:  blockSize,
		blockLocks: locks,
		numBlocks:  numBlocks,
		exists:     util.NewBitfield(numBlocks),
	}
}

func (i *BlockCache) tryCache(b uint, buffer []byte) bool {
	i.blocksLock.Lock()
	defer i.blocksLock.Unlock()

	// First try updating the cache entry
	d, ok := i.blocks[b]
	if ok {
		d.data = buffer
		d.lastAccess = time.Now()
		return true
	}

	// Might need to add a new cache entry
	if len(i.blocks) < i.maxBlocks {
		// Add the cached block
		i.blocks[b] = &blockInfo{
			lastAccess: time.Now(),
			data:       buffer,
		}
		i.exists.SetBit(int(b))
		return true
	}

	/*
		allBlocks := make([]uint, 0)
		for v := range i.blocks {
			allBlocks = append(allBlocks, v)
		}
		sort.SliceStable(allBlocks, func(index1 int, index2 int) bool {
			b1 := allBlocks[index1]
			b2 := allBlocks[index2]
			return i.blocks[b1].lastAccess.Before(i.blocks[b2].lastAccess)
		})
	*/
	// Now we have a sorted list, we can get rid of some number of blocks from the cache...

	// For now, when the cache gets full, that's it. It'll of course still speed up the blocks in the cache,
	// but none of the blocks outside of it.
	return false
}

func (i *BlockCache) readBlock(b uint) ([]byte, error) {
	// Check if we have it in our cache...
	i.blocksLock.Lock()
	binfo, ok := i.blocks[b]
	if ok {
		binfo.lastAccess = time.Now()
		i.blocksLock.Unlock()
		atomic.AddUint64(&i.metricReadHits, 1)
		return binfo.data, nil
	}
	i.blocksLock.Unlock()

	// Read it from source
	atomic.AddUint64(&i.metricReadMisses, 1)
	buffer := make([]byte, i.blockSize)
	_, err := i.prov.ReadAt(buffer, int64(b*uint(i.blockSize)))
	i.tryCache(b, buffer) // Try to add it to our cache
	return buffer, err
}

func (i *BlockCache) writeBlock(b uint, buffer []byte) error {
	if i.tryCache(b, buffer) { // Try to add it to our cache
		atomic.AddUint64(&i.metricWriteHits, 1)
		return nil
	}

	atomic.AddUint64(&i.metricWriteMisses, 1)
	_, err := i.prov.WriteAt(buffer, int64(b*uint(i.blockSize)))
	return err
}

func (i *BlockCache) flushBlocks() error {
	var errs error

	// Lock all blocks
	for b := 0; b < i.numBlocks; b++ {
		i.blockLocks[b].Lock()
	}

	currentStartBlock := 0
	currentData := make([]byte, 0)
	for b := 0; b < i.numBlocks; b++ {
		d, ok := i.blocks[uint(b)]
		if !ok {
			// Flush the current range and start fresh
			if len(currentData) > 0 {
				_, e := i.prov.WriteAt(currentData, int64(currentStartBlock)*int64(i.blockSize))
				if e != nil {
					errs = errors.Join(errs, e)
				} else {
					atomic.AddUint64(&i.metricFlushBlocks, 1)
				}
				currentData = make([]byte, 0)
			}
		} else {
			if len(currentData) == 0 {
				currentStartBlock = b
			}
			// Add it on...
			currentData = append(currentData, d.data...)
		}
		delete(i.blocks, uint(b))
	}
	// Flush the last block...
	if len(currentData) > 0 {
		_, e := i.prov.WriteAt(currentData, int64(currentStartBlock)*int64(i.blockSize))
		if e != nil {
			errs = errors.Join(errs, e)
		} else {
			atomic.AddUint64(&i.metricFlushBlocks, 1)
		}
	}

	// Unlock all blocks
	for b := 0; b < i.numBlocks; b++ {
		i.blockLocks[b].Unlock()
	}

	return errs
}

func (i *BlockCache) ReadAt(buffer []byte, offset int64) (int, error) {
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

	// If we don't have it all in the cache, do a single read, and then update the cache...
	if !i.exists.BitsSet(bStart, bEnd) {
		n, err := i.prov.ReadAt(buffer, offset)
		for bb := bStart; bb < bEnd; bb++ {
			blockOffset := int64(bb) * int64(i.blockSize)
			if blockOffset >= offset {
				// Partial read at the end
				if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
				} else {
					// Complete block reads in the middle
					s := blockOffset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					i.tryCache(bb, buffer[s:e])
				}
			} else {
				// Partial read at the start
			}
		}
		return n, err
	}

	blocks := bEnd - bStart
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	for bb := bStart; bb < bEnd; bb++ {
		go func(b uint) {
			i.blockLocks[b].RLock()
			defer i.blockLocks[b].RUnlock()
			count := 0
			blockData, err := i.readBlock(b)
			if err == nil {
				blockOffset := int64(b) * int64(i.blockSize)
				if blockOffset >= offset {
					// Partial read at the end
					if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
						count = copy(buffer[blockOffset-offset:bufferEnd], blockData)
					} else {
						// Complete block reads in the middle
						s := blockOffset - offset
						e := s + int64(i.blockSize)
						if e > int64(len(buffer)) {
							e = int64(len(buffer))
						}
						count = copy(buffer[s:e], blockData)
					}
				} else {
					// Partial read at the start
					count = copy(buffer[:bufferEnd], blockData[offset-blockOffset:])
				}
			}
			errs <- err
			counts <- count
		}(bb)
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

func (i *BlockCache) WriteAt(buffer []byte, offset int64) (int, error) {
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

	// If we don't have it all in the cache, do a single write, and then update the cache...
	if !i.exists.BitsSet(bStart, bEnd) {
		n, err := i.prov.WriteAt(buffer, offset)
		for bb := bStart; bb < bEnd; bb++ {
			blockOffset := int64(bb) * int64(i.blockSize)
			if blockOffset >= offset {
				// Partial read at the end
				if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
				} else {
					// Complete block reads in the middle
					s := blockOffset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					i.tryCache(bb, buffer[s:e])
				}
			} else {
				// Partial read at the start
			}
		}
		return n, err
	}

	blocks := bEnd - bStart
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	for bb := bStart; bb < bEnd; bb++ {
		go func(b uint) {
			i.blockLocks[b].Lock()
			defer i.blockLocks[b].Unlock()

			var err error
			count := 0
			blockOffset := int64(b) * int64(i.blockSize)
			if blockOffset >= offset {
				// Partial read at the end
				if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
					blockData, e := i.readBlock(b)
					if e == nil {
						count = copy(blockData, buffer[blockOffset-offset:bufferEnd])
						err = i.writeBlock(b, blockData)
					} else {
						err = e
					}
				} else {
					// Complete block write in the middle
					s := blockOffset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					err = i.writeBlock(b, buffer[s:e])
					count = i.blockSize
				}
			} else {
				// Partial write at the start
				blockData, e := i.readBlock(b)
				if e == nil {
					count = copy(blockData[offset-blockOffset:], buffer[:bufferEnd])
					err = i.writeBlock(b, blockData)
				} else {
					err = e
				}
			}

			errs <- err
			counts <- count
		}(bb)
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

func (i *BlockCache) Flush() error {
	err := i.flushBlocks()
	return errors.Join(err, i.prov.Flush())
}

func (i *BlockCache) Size() uint64 {
	return i.prov.Size()
}

func (i *BlockCache) Close() error {
	err := i.flushBlocks()
	return errors.Join(err, i.prov.Close())
}

func (i *BlockCache) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
