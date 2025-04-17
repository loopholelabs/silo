package writecache

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type WriteCache struct {
	prov      storage.Provider
	ctx       context.Context
	cancel    context.CancelFunc
	blockSize int
	blocks    []*BlockInfo
	totalData int64 // Current count of data stored here
	maxData   int64 // Maximum amount of data we want to cache
	minData   int64 // Minimum amount of data after a partial flush
}

type BlockInfo struct {
	block     uint64
	lock      sync.Mutex
	writes    []*WriteData
	bytes     int       // How much data is stored here
	lastFlush time.Time // When was this block last flushed?

	minOffset int64
	maxOffset int64
}

type WriteData struct {
	offset int64
	data   []byte
}

// Add some write data to a block
func (bi *BlockInfo) WriteAt(buffer []byte, offset int64) {
	bi.lock.Lock()
	defer bi.lock.Unlock()
	bi.writes = append(bi.writes, &WriteData{
		offset: offset,
		data:   buffer,
	})
	bi.bytes += len(buffer)

	// Update data extents
	if offset < bi.minOffset {
		bi.minOffset = offset
	}
	if offset+int64(len(buffer)) > bi.maxOffset {
		bi.maxOffset = offset + int64(len(buffer))
	}
}

// Clear all data from the BlockInfo
func (bi *BlockInfo) Clear(blockSize int64) {
	bi.writes = make([]*WriteData, 0)
	bi.lastFlush = time.Now()
	bi.bytes = 0
	bi.minOffset = blockSize
	bi.maxOffset = 0
}

func NewWriteCache(blockSize int, prov storage.Provider, maxData int64, minData int64, flushPeriod time.Duration) *WriteCache {
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
		ctx:       ctx,
		cancel:    cancel,
		prov:      prov,
		blockSize: blockSize,
		blocks:    blocks,
		maxData:   maxData,
		minData:   minData,
	}

	// Set something up to periodically flush writes...
	go func() {
		ticker := time.NewTicker(flushPeriod)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				wc.flushSome(minData)
			}
		}
	}()

	return wc
}

func (i *WriteCache) ReadAt(buffer []byte, offset int64) (int, error) {
	end := offset + int64(len(buffer))

	bStart := offset / int64(i.blockSize)
	bEnd := ((end - 1) / int64(i.blockSize)) + 1

	// In this first implementation, we simply flush blocks before we read from them.
	for b := bStart; b < bEnd; b++ {
		err := i.flushBlock(int(b))
		if err != nil {
			return 0, err
		}
	}
	return i.prov.ReadAt(buffer, offset)
}

func (i *WriteCache) WriteAt(buffer []byte, offset int64) (int, error) {
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

		// We have too much data, lets flush some of it
		// TODO: Instead of flushing *everything*, we could just flush the most active blocks
		// this would smooth out the writes over time...
		if atomic.LoadInt64(&i.totalData)+int64(len(blockData)) >= i.maxData {
			i.flushSome(i.minData)
		}

		// Add the write data to the block
		i.blocks[b].WriteAt(blockData, blockOffset)
		atomic.AddInt64(&i.totalData, int64(len(blockData)))
	}

	return len(buffer), nil
}

/**
 * Flush a block out to the provider
 *
 */
func (i *WriteCache) flushBlock(b int) error {
	bi := i.blocks[b]
	bi.lock.Lock()
	if len(bi.writes) > 0 {
		// We need to flush these writes...

		// Find out the extents
		bf := util.NewBitfield(i.blockSize)
		for _, w := range bi.writes {
			bf.SetBits(uint(w.offset), uint(w.offset+int64(len(w.data))))
		}
		gaps := !bf.BitsSet(uint(bi.minOffset), uint(bi.maxOffset))

		blockBuffer := make([]byte, bi.maxOffset-bi.minOffset)

		// There are gaps in the write data, which means we need to do a read
		if gaps {
			_, err := i.prov.ReadAt(blockBuffer, int64(b*i.blockSize)+bi.minOffset)
			if err != nil {
				bi.lock.Unlock()
				return err
			}
		}

		// Now merge in the writes to the blockBuffer...
		for _, w := range bi.writes {
			copy(blockBuffer[w.offset-bi.minOffset:], w.data)
			atomic.AddInt64(&i.totalData, -int64(len(w.data)))
		}

		// And write the data back
		_, err := i.prov.WriteAt(blockBuffer, int64(b*i.blockSize)+bi.minOffset)
		if err != nil {
			bi.lock.Unlock()
			return err
		}

		bi.Clear(int64(i.blockSize))
	}
	bi.lock.Unlock()
	return nil
}

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
 * Flush some data out of the writeCache.
 * Data is flushed until some target is reached, starting with the block with most data
 */
func (i *WriteCache) flushSome(target int64) error {
	blocks := make([]*BlockInfo, len(i.blocks))
	for b, bi := range i.blocks {
		blocks[b] = bi
	}

	// Now sort it
	sort.Slice(blocks, func(i int, j int) bool {
		return blocks[i].bytes > blocks[j].bytes
	})

	// Flush from biggest to smallest
	for _, bi := range blocks {
		if bi.bytes > 0 {
			err := i.flushBlock(int(bi.block))
			if err != nil {
				return nil
			}
			if atomic.LoadInt64(&i.totalData) < target {
				break
			}
		}
	}
	return nil
}

func (i *WriteCache) Size() uint64 {
	return i.prov.Size()
}

func (i *WriteCache) Close() error {
	i.cancel() // We don't need to be flushing things any more.

	return errors.Join(i.Flush(), i.prov.Close())
}

func (i *WriteCache) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
