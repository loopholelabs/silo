package writecache

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type WriteCache struct {
	uuid string
	storage.ProviderWithEvents
	prov              storage.Provider
	ctx               context.Context
	cancel            context.CancelFunc
	enabled           atomic.Bool
	blockSize         int
	blocks            []*BlockInfo
	totalData         int64 // Current count of data stored here
	maxData           int64 // Maximum amount of data we want to cache
	minData           int64 // Minimum amount of data after a partial flush
	minimizeReadBytes bool  // Should we try to minimize read bytes (cpu cost)

	flushLock sync.Mutex
	writeLock sync.RWMutex
}

type BlockInfo struct {
	block  uint64
	lock   sync.Mutex
	writes []*WriteData
	bytes  int64 // How much data is stored here

	minOffset int64
	maxOffset int64
}

type WriteData struct {
	offset int64
	data   []byte
}

// Add some write data to a block
func (bi *BlockInfo) writeAt(buffer []byte, offset int64) int {
	bi.lock.Lock()
	defer bi.lock.Unlock()

	dataChange := 0

	// Optimization: remove anything this overwrites completely
	newWrites := make([]*WriteData, 0)
	for _, w := range bi.writes {
		if w.offset >= offset && (w.offset+int64(len(w.data))) <= (offset+int64(len(buffer))) {
			// This is enclosed inside the new write, so it's now irrelevant, and can be discarded.
			dataChange -= len(w.data)
			atomic.AddInt64(&bi.bytes, int64(-len(w.data)))
		} else {
			newWrites = append(newWrites, w)
		}
	}

	// Copy the data. Since we're caching it, we don't want to assume the buffer won't be reused by the consumer.
	cdata := make([]byte, len(buffer))
	copy(cdata, buffer)

	newWrites = append(newWrites, &WriteData{
		offset: offset,
		data:   cdata,
	})
	bi.writes = newWrites
	atomic.AddInt64(&bi.bytes, int64(len(buffer)))
	dataChange += len(buffer)

	// Update data extents
	if offset < bi.minOffset {
		bi.minOffset = offset
	}
	if offset+int64(len(buffer)) > bi.maxOffset {
		bi.maxOffset = offset + int64(len(buffer))
	}
	return dataChange
}

// Clear all data from the BlockInfo
func (bi *BlockInfo) Clear(blockSize int64) {
	bi.writes = make([]*WriteData, 0)
	atomic.StoreInt64(&bi.bytes, 0)
	bi.minOffset = blockSize
	bi.maxOffset = 0
}

// Relay events to embedded StorageProvider
func (i *WriteCache) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	if eventType == storage.EventTypeCowGetBlocks {
		i.Disable() // Lock writes, disable future caching, and flush the cache.
		// We *need* to flush here, so that Cow knows which blocks are changed etc
		// We disable the cache for the migration, because we need to make sure ALL data is migrated.
	}

	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

type Config struct {
	MinData           int64
	MaxData           int64
	FlushPeriod       time.Duration
	MinimizeReadBytes bool
}

/**
 * NewWriteCache creates a new write caching layer around the provided storage.Provider.
 * It buffers writes up to the configured size limits before flushing them to the underlying provider.
 *
 */
func NewWriteCache(blockSize int, prov storage.Provider, conf *Config) *WriteCache {
	numBlocks := (prov.Size() + uint64(blockSize) - 1) / uint64(blockSize)

	blocks := make([]*BlockInfo, numBlocks)
	for i := uint64(0); i < numBlocks; i++ {
		blocks[i] = &BlockInfo{
			block:     i,
			writes:    make([]*WriteData, 0),
			maxOffset: 0,
			minOffset: int64(blockSize),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	wc := &WriteCache{
		uuid:              uuid.NewString(),
		ctx:               ctx,
		cancel:            cancel,
		prov:              prov,
		blockSize:         blockSize,
		blocks:            blocks,
		maxData:           conf.MaxData,
		minData:           conf.MinData,
		enabled:           atomic.Bool{},
		minimizeReadBytes: conf.MinimizeReadBytes,
	}

	wc.enabled.Store(true) // Enable this cache

	// Set something up to periodically flush writes...
	go func() {
		ticker := time.NewTicker(conf.FlushPeriod)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				wc.flushSome(conf.MinData)
			}
		}
	}()

	return wc
}

/**
 * ReadAt implements the Provider interface.
 * This implementation flushes all blocks required by the read operation before
 * forwarding the request to the underlying provider.
 */
func (i *WriteCache) ReadAt(buffer []byte, offset int64) (int, error) {
	end := offset + int64(len(buffer))

	bStart := offset / int64(i.blockSize)
	bEnd := ((end - 1) / int64(i.blockSize)) + 1

	// In this first implementation, we simply flush blocks before we read from them, to make sure
	// the data is correct.
	for b := bStart; b < bEnd; b++ {
		err := i.flushBlock(int(b))
		if err != nil {
			return 0, err
		}
	}
	return i.prov.ReadAt(buffer, offset)
}

/**
 * WriteAt implements the Provider interface.
 */
func (i *WriteCache) WriteAt(buffer []byte, offset int64) (int, error) {
	// Incase a cache disable / flush op is going on
	// NOTE we are intentionally using RLock/RUnlock here. We don't mind if concurrent WriteAt
	// go on, as long as a disable cache / flush op isn't going on.
	i.writeLock.RLock()
	defer i.writeLock.RUnlock()

	// Pass through
	if !i.enabled.Load() {
		return i.prov.WriteAt(buffer, offset)
	}

	end := offset + int64(len(buffer))

	bStart := offset / int64(i.blockSize)
	bEnd := ((end - 1) / int64(i.blockSize)) + 1

	// Go through blocks
	for b := bStart; b < bEnd; b++ {
		bOffset := b * int64(i.blockSize)
		// Now work out what data, and what offset to use here...

		blockOffset := int64(0)
		blockData := buffer
		if bOffset >= offset { // Not the first (partial) block
			blockData = buffer[bOffset-offset:]
		} else { // First partial block
			blockOffset = offset - bOffset
		}
		if (blockOffset + int64(len(blockData))) > int64(i.blockSize) { // Clamp it to blockSize
			blockData = blockData[:(int64(i.blockSize) - blockOffset)]
		}

		// We lock here so that only one is doing the flush...
		i.flushLock.Lock()
		// We have too much data, lets flush some of it
		if atomic.LoadInt64(&i.totalData)+int64(len(blockData)) >= i.maxData {
			i.flushSome(i.minData)
		}

		// Add the write data to the block, and update totalData
		dataChange := i.blocks[b].writeAt(blockData, blockOffset)
		atomic.AddInt64(&i.totalData, int64(dataChange))

		i.flushLock.Unlock()
	}

	return len(buffer), nil
}

/**
 * flushBlock flushes a specific block out to the provider
 *
 */
func (i *WriteCache) flushBlock(b int) error {
	bi := i.blocks[b]
	bi.lock.Lock()
	if len(bi.writes) > 0 {
		// We need to flush these writes...

		readStart := uint(bi.minOffset)
		readEnd := uint(bi.maxOffset)
		if i.minimizeReadBytes {
			// Check if we need to do a read, and the extents
			// TODO: Do this as we receive the writes?
			bf := util.NewBitfield(i.blockSize)
			for _, w := range bi.writes {
				bf.SetBits(uint(w.offset), uint(w.offset+int64(len(w.data))))
			}
			readStart, readEnd = bf.CollectZeroExtents(uint(bi.minOffset), uint(bi.maxOffset))
		}

		blockBuffer := make([]byte, bi.maxOffset-bi.minOffset)

		// There are gaps in the write data, which means we need to do a read
		// We can read only the data we need though
		if readStart != readEnd {
			srcOffset := int64(b*i.blockSize) + int64(readStart)
			_, err := i.prov.ReadAt(blockBuffer[int64(readStart)-bi.minOffset:int64(readEnd)-bi.minOffset], srcOffset)
			if err != nil {
				bi.lock.Unlock()
				return err
			}
		}

		// Make a copy of the current data.
		orgBlockBuffer := make([]byte, len(blockBuffer))
		copy(orgBlockBuffer, blockBuffer)

		// Now merge in the writes to the blockBuffer...
		for _, w := range bi.writes {
			copy(blockBuffer[w.offset-bi.minOffset:], w.data)
			atomic.AddInt64(&i.totalData, -int64(len(w.data)))
		}

		// Check where the data has changed...
		minChanged := len(blockBuffer)
		maxChanged := 0
		somethingChanged := false
		for i := 0; i < len(blockBuffer); i++ {
			if blockBuffer[i] != orgBlockBuffer[i] {
				if i+1 > maxChanged {
					maxChanged = i + 1
				}
				if i < minChanged {
					minChanged = i
				}
				somethingChanged = true
			}
		}

		if somethingChanged {
			// Here, we can write changedMin - changedMax
			_, err := i.prov.WriteAt(blockBuffer[minChanged:maxChanged], int64(b*i.blockSize)+bi.minOffset+int64(minChanged))

			// And write the data back
			// _, err := i.prov.WriteAt(blockBuffer, int64(b*i.blockSize)+bi.minOffset)
			if err != nil {
				bi.lock.Unlock()
				return err
			}
		}

		bi.Clear(int64(i.blockSize))
	}
	bi.lock.Unlock()
	return nil
}

/**
 * Flush implements provider
 *
 */
func (i *WriteCache) Flush() error {
	// Here we need to flush all the blocks out...
	for b := range i.blocks {
		err := i.flushBlock(b)
		if err != nil {
			return err
		}
	}
	return nil
}

/**
 * flushSome flushes some data out of the writeCache.
 * Data is flushed until some target is reached, starting with the block with most data
 * TODO: Optimize this so we're not sorting lots
 */
func (i *WriteCache) flushSome(target int64) error {
	// Make a copy so we can sort it
	blocks := make([]*BlockInfo, len(i.blocks))
	copy(blocks, i.blocks)

	// Now sort it.
	// We must lock the blocks to avoid concurrent access.
	sort.Slice(blocks, func(i int, j int) bool {
		return atomic.LoadInt64(&blocks[i].bytes) > atomic.LoadInt64(&blocks[j].bytes)
	})

	// Flush from biggest to smallest
	for _, bi := range blocks {
		err := i.flushBlock(int(bi.block))
		if err != nil {
			return nil
		}
		if atomic.LoadInt64(&i.totalData) < target {
			break
		}
	}
	return nil
}

/**
 * Size implemnts provider
 *
 */
func (i *WriteCache) Size() uint64 {
	return i.prov.Size()
}

/**
 * Close implements provider
 *
 */
func (i *WriteCache) Close() error {
	i.cancel()        // We don't need to be flushing things any more.
	err1 := i.Flush() // Flush anything else out
	i.Disable()       // Disable any more caching behaviour
	err2 := i.prov.Close()
	if err1 == nil && err2 == nil {
		return nil
	}

	return errors.Join(err1, err2)
}

/**
 * CancelWrites implements provider
 *
 */
func (i *WriteCache) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}

/**
 * Disable disables the cache, and waits for any pending writes to be completed
 * this is useful for example when a migration starts.
 */
func (i *WriteCache) Disable() {
	// Disable caching
	i.enabled.Store(false)

	i.writeLock.Lock()
	defer i.writeLock.Unlock()

	// FLUSH everything NOW
	i.Flush()
}

/**
 * Enable enables the writeCache after it's been disabled.
 *
 */
func (i *WriteCache) Enable() {
	i.writeLock.Lock()
	defer i.writeLock.Unlock()

	// Now enable caching
	i.enabled.Store(true)
}
