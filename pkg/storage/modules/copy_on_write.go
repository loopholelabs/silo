package modules

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/bitfield"
)

type CopyOnWrite struct {
	storage.ProviderWithEvents
	source     storage.Provider
	cache      storage.Provider
	exists     *bitfield.Bitfield
	nonzero    *bitfield.Bitfield
	size       uint64
	blockSize  int
	CloseFn    func()
	writeLocks []*sync.Mutex
	sharedBase bool

	wg        sync.WaitGroup
	closing   bool
	closeLock sync.Mutex

	metricZeroReadOps           uint64
	metricZeroReadBytes         uint64
	metricZeroPreWriteReadOps   uint64
	metricZeroPreWriteReadBytes uint64
}

type CopyOnWriteMetrics struct {
	MetricNonZeroSize           uint64
	MetricSize                  uint64
	MetricOverlaySize           uint64
	MetricZeroReadOps           uint64
	MetricZeroReadBytes         uint64
	MetricZeroPreWriteReadOps   uint64
	MetricZeroPreWriteReadBytes uint64
}

func (i *CopyOnWrite) GetMetrics() *CopyOnWriteMetrics {
	overlaySize := uint64(i.exists.Count(0, i.exists.Length())) * uint64(i.blockSize)
	return &CopyOnWriteMetrics{
		MetricSize:                  i.size,
		MetricNonZeroSize:           uint64(i.blockSize * i.nonzero.Count(0, i.nonzero.Length())),
		MetricOverlaySize:           overlaySize,
		MetricZeroReadOps:           i.metricZeroReadOps,
		MetricZeroReadBytes:         i.metricZeroReadBytes,
		MetricZeroPreWriteReadOps:   i.metricZeroPreWriteReadOps,
		MetricZeroPreWriteReadBytes: i.metricZeroPreWriteReadBytes,
	}
}

/**
 * Check exactly how much data has changed in this COW
 *
 */
func (i *CopyOnWrite) GetDifference() (int64, int64, error) {
	blocksChanged := int64(0)
	bytesChanged := int64(0)

	numBlocks := int(i.exists.Length())
	for b := 0; b < numBlocks; b++ {
		if i.exists.BitSet(b) {
			// Check the data here...
			bufferBase := make([]byte, i.blockSize)
			bufferOverlay := make([]byte, i.blockSize)
			nOverlay, err := i.cache.ReadAt(bufferOverlay, int64(b*i.blockSize))
			if err != nil {
				return 0, 0, err
			}
			nBase, err := i.source.ReadAt(bufferBase, int64(b*i.blockSize))
			if err != nil {
				return 0, 0, err
			}

			if nBase != nOverlay {
				return 0, 0, fmt.Errorf("ReadAt returned different values %d %d", nBase, nOverlay)
			}
			// Now check the data
			same := true
			for n := 0; n < nBase; n++ {
				if bufferBase[n] != bufferOverlay[n] {
					same = false
					bytesChanged++
				}
			}

			if !same {
				blocksChanged++
			}
		}
	}
	return blocksChanged, bytesChanged, nil
}

var ErrClosed = errors.New("device is closing or already closed")

func (i *CopyOnWrite) lockAll() {
	for _, l := range i.writeLocks {
		l.Lock()
	}
}

func (i *CopyOnWrite) unlockAll() {
	for _, l := range i.writeLocks {
		l.Unlock()
	}
}

// Relay events to embedded StorageProvider
func (i *CopyOnWrite) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	if i.sharedBase {
		if eventType == storage.EventTypeCowGetBlocks {
			i.lockAll() // Just makes sure that no writes are in progress while we snapshot.
			unrequired := i.exists.CollectZeroes(0, i.exists.Length())
			i.unlockAll()

			orgData := storage.SendSiloEvent(i.source, eventType, eventData)

			// Now we need to combine the two...
			if len(orgData) == 1 {
				orgBlockMap := make(map[uint]bool)
				orgUnrequired := orgData[0].([]uint)
				for _, b := range orgUnrequired {
					orgBlockMap[b] = true
				}
				// If we don't require it, AND any lower layers don't require it, then include it in the list.
				unrequiredAll := make([]uint, 0)
				for _, b := range unrequired {
					if orgBlockMap[b] {
						unrequiredAll = append(unrequiredAll, b)
					}
				}

				return []storage.EventReturnData{
					unrequiredAll,
				}
			}

			// Lower layers aren't cow, so we just return our own.
			return []storage.EventReturnData{
				unrequired,
			}
		}
	}

	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	data = append(data, storage.SendSiloEvent(i.cache, eventType, eventData)...)
	return append(data, storage.SendSiloEvent(i.source, eventType, eventData)...)
}

func NewCopyOnWrite(source storage.Provider, cache storage.Provider, blockSize int, sharedBase bool, hashes [][sha256.Size]byte) *CopyOnWrite {
	numBlocks := (source.Size() + uint64(blockSize) - 1) / uint64(blockSize)
	locks := make([]*sync.Mutex, numBlocks)
	for t := 0; t < int(numBlocks); t++ {
		locks[t] = &sync.Mutex{}
	}

	nonzero := bitfield.NewBitfield(int(numBlocks))

	zeroHash := make([]byte, sha256.Size)

	if hashes != nil {
		// We have hash info, and if the hash is all zeroes, it means the data is all zeroes.
		for b := 0; b < int(numBlocks); b++ {
			if !bytes.Equal(hashes[b][:], zeroHash) {
				nonzero.SetBit(b)
			}
		}
	} else {
		// We have no hash data, so we'll assume all the data is non-zero
		nonzero.SetBits(0, nonzero.Length())
	}

	return &CopyOnWrite{
		source:     source,
		cache:      cache,
		exists:     bitfield.NewBitfield(int(numBlocks)),
		nonzero:    nonzero,
		writeLocks: locks,
		size:       source.Size(),
		blockSize:  blockSize,
		CloseFn:    func() {},
		sharedBase: sharedBase,
		closing:    false,
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
	i.closeLock.Lock()
	if i.closing {
		i.closeLock.Unlock()
		return 0, ErrClosed
	}
	i.wg.Add(1)
	i.closeLock.Unlock()

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

	for bb := bStart; bb < bEnd; bb++ {
		go func(b uint) {
			count := 0
			blockOffset := int64(b) * int64(i.blockSize)
			var err error
			if blockOffset >= offset {
				// Partial read at the end
				if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
					num := bufferEnd - (blockOffset - offset)
					if i.exists.BitSet(int(b)) {
						count, err = i.cache.ReadAt(buffer[blockOffset-offset:bufferEnd], blockOffset)
					} else {
						blockBuffer := make([]byte, num)

						if i.nonzero.BitSet(int(b)) {
							// Read existing data
							_, err = i.source.ReadAt(blockBuffer, blockOffset)
						} else {
							atomic.AddUint64(&i.metricZeroReadOps, 1)
							atomic.AddUint64(&i.metricZeroReadBytes, uint64(bufferEnd-(blockOffset-offset)))
							err = nil
						}
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
						if i.nonzero.BitSet(int(b)) {
							_, err = i.source.ReadAt(buffer[s:e], blockOffset)
						} else {
							// Clear
							for p := s; p < e; p++ {
								buffer[p] = 0
							}
							atomic.AddUint64(&i.metricZeroReadOps, 1)
							atomic.AddUint64(&i.metricZeroReadBytes, uint64(i.blockSize))
							err = nil
						}
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
					blockBuffer := make([]byte, i.blockSize-int(offset-blockOffset))
					if i.nonzero.BitSet(int(b)) {
						_, err = i.source.ReadAt(blockBuffer, offset)
					} else {
						err = nil
						atomic.AddUint64(&i.metricZeroReadOps, 1)
						atomic.AddUint64(&i.metricZeroReadBytes, uint64(int64(i.blockSize)-(offset-blockOffset)))
					}
					if err == nil {
						count = copy(buffer[:bufferEnd], blockBuffer)
					}
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

func (i *CopyOnWrite) WriteAt(buffer []byte, offset int64) (int, error) {
	i.closeLock.Lock()
	if i.closing {
		i.closeLock.Unlock()
		return 0, ErrClosed
	}
	i.wg.Add(1)
	i.closeLock.Unlock()

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

	// Special case where all the data is in the cache
	if i.exists.BitsSet(bStart, bEnd) {
		n, err := i.cache.WriteAt(buffer, offset)
		return n, err
	}

	blocks := bEnd - bStart
	errs := make(chan error, blocks)
	counts := make(chan int, blocks)

	// Now we have a series of non-overlapping writes. Do them concurrently
	for bb := bStart; bb < bEnd; bb++ {
		go func(b uint) {
			i.writeLocks[b].Lock()
			defer i.writeLocks[b].Unlock()

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
						if i.nonzero.BitSet(int(b)) {
							_, err = i.source.ReadAt(blockBuffer, blockOffset)
						} else {
							atomic.AddUint64(&i.metricZeroPreWriteReadOps, 1)
							atomic.AddUint64(&i.metricZeroPreWriteReadBytes, uint64(bufferEnd-(blockOffset-offset)))
							err = nil
						}
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
					if i.nonzero.BitSet(int(b)) {
						_, err = i.source.ReadAt(blockBuffer, blockOffset)
					} else {
						err = nil
						atomic.AddUint64(&i.metricZeroPreWriteReadOps, 1)
						atomic.AddUint64(&i.metricZeroPreWriteReadBytes, uint64(int64(i.blockSize)-(offset-blockOffset)))
					}
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

func (i *CopyOnWrite) Flush() error {
	return nil
}

func (i *CopyOnWrite) Size() uint64 {
	return i.source.Size()
}

func (i *CopyOnWrite) Close() error {
	//
	i.closeLock.Lock()
	i.closing = true
	i.closeLock.Unlock()

	i.wg.Wait() // Wait for any pending reads/writes to complete
	i.cache.Close()
	i.source.Close()
	i.CloseFn()
	return nil
}

func (i *CopyOnWrite) CancelWrites(offset int64, length int64) {
	i.cache.CancelWrites(offset, length)
}
