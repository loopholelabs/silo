package modules

import (
	"errors"
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
	writeLock  sync.Mutex // TODO: Do this at the block level to increase throughput
	sharedBase bool

	readBeforeWrites bool

	wg        sync.WaitGroup
	closing   bool
	closeLock sync.Mutex
}

var ErrClosed = errors.New("device is closing or already closed")

// Relay events to embedded StorageProvider
func (i *CopyOnWrite) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	if i.sharedBase {
		if eventType == storage.EventTypeCowGetBlocks {
			i.writeLock.Lock() // Just makes sure that no writes are in progress while we snapshot.
			unrequired := i.exists.CollectZeroes(0, i.exists.Length())
			i.writeLock.Unlock()
			return []storage.EventReturnData{
				unrequired,
			}
		}
	}

	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	data = append(data, storage.SendSiloEvent(i.cache, eventType, eventData)...)
	return append(data, storage.SendSiloEvent(i.source, eventType, eventData)...)
}

type CopyOnWriteConfig struct {
	SharedBase      bool
	ReadBeforeWrite bool
}

func NewCopyOnWrite(source storage.Provider, cache storage.Provider, blockSize int, conf *CopyOnWriteConfig) *CopyOnWrite {
	numBlocks := (source.Size() + uint64(blockSize) - 1) / uint64(blockSize)
	return &CopyOnWrite{
		source:           source,
		cache:            cache,
		exists:           util.NewBitfield(int(numBlocks)),
		size:             source.Size(),
		blockSize:        blockSize,
		CloseFn:          func() {},
		sharedBase:       conf.SharedBase,
		readBeforeWrites: conf.ReadBeforeWrite,
		closing:          false,
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
	i.closeLock.Lock()
	if i.closing {
		i.closeLock.Unlock()
		return 0, ErrClosed
	}
	i.wg.Add(1)
	i.closeLock.Unlock()

	defer i.wg.Done()

	i.writeLock.Lock()
	defer i.writeLock.Unlock()

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
						if i.readBeforeWrites {
							for o := int64(0); o < (bufferEnd - (blockOffset - offset)); o++ {
								if blockBuffer[o] != buffer[blockOffset-offset+o] {
									// Data has changed. Write it
									// Merge in data
									count = copy(blockBuffer, buffer[blockOffset-offset:bufferEnd])
									// Write back to cache
									_, err = i.cache.WriteAt(blockBuffer, blockOffset)
									i.exists.SetBit(int(b))
									break
								}
							}
						} else {
							// Merge in data
							count = copy(blockBuffer, buffer[blockOffset-offset:bufferEnd])
							// Write back to cache
							_, err = i.cache.WriteAt(blockBuffer, blockOffset)
							i.exists.SetBit(int(b))
						}
					}
				}
			} else {
				// Complete block writes in the middle
				s := blockOffset - offset
				e := s + int64(i.blockSize)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}

				err = i.writeBlock(b, buffer[s:e])
				if err == nil {
					count = i.blockSize
				}
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
					if i.readBeforeWrites {
						for o := offset - blockOffset; o < int64(i.blockSize); o++ {
							if blockBuffer[o] != buffer[o-(offset-blockOffset)] {

								count = copy(blockBuffer[offset-blockOffset:], buffer[:bufferEnd])
								_, err = i.cache.WriteAt(blockBuffer, blockOffset)
								i.exists.SetBit(int(b))
								break
							}
						}
					} else {
						// Merge in data
						count = copy(blockBuffer[offset-blockOffset:], buffer[:bufferEnd])
						_, err = i.cache.WriteAt(blockBuffer, blockOffset)
						i.exists.SetBit(int(b))
					}
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

func (i *CopyOnWrite) writeBlock(b uint, data []byte) error {
	blockOffset := int64(b) * int64(i.blockSize)

	var err error
	if i.readBeforeWrites {
		baseData := make([]byte, i.blockSize)
		_, err = i.source.ReadAt(baseData, blockOffset)
		if err == nil {
			// Check if the data changed
			for f := 0; f < i.blockSize; f++ {
				if baseData[f] != data[f] {
					// Data has changed, write it.
					_, err = i.cache.WriteAt(data, blockOffset)
					i.exists.SetBit(int(b))
					break
				}
			}
		}
	} else {
		_, err = i.cache.WriteAt(data, blockOffset)
		i.exists.SetBit(int(b))
	}
	return err
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
