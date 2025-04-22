package sources

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage"
)

const blockHeaderSize = 8

var errNoBlock = errors.New("no such block")

/**
 * Simple sparse file storage provider
 *
 * - Reads return error if no data has been written for a block yet.
 * - Partial block reads supported as long as the blocks exist.
 * - Partial block writes supported as long as the blocks exist (Have already been written to completely).
 *
 */
type FileStorageSparse struct {
	storage.ProviderWithEvents
	f           string
	fp          *os.File
	size        uint64
	blockSize   int
	offsetsLock sync.RWMutex
	offsets     map[uint]uint64
	writeLocks  []sync.RWMutex
	currentSize uint64
	wg          sync.WaitGroup
}

func NewFileStorageSparseCreate(f string, size uint64, blockSize int) (*FileStorageSparse, error) {
	fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	numBlocks := (int(size) + blockSize - 1) / blockSize
	locks := make([]sync.RWMutex, numBlocks)

	return &FileStorageSparse{
		f:           f,
		fp:          fp,
		size:        size,
		blockSize:   blockSize,
		offsets:     make(map[uint]uint64),
		currentSize: 0,
		writeLocks:  locks,
	}, nil
}

func NewFileStorageSparse(f string, size uint64, blockSize int) (*FileStorageSparse, error) {
	fp, err := os.OpenFile(f, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	numBlocks := (int(size) + blockSize - 1) / blockSize
	locks := make([]sync.RWMutex, numBlocks)

	// Scan through the file and get the offsets...
	offsets := make(map[uint]uint64)

	header := make([]byte, blockHeaderSize)
	p := int64(0)
	for {
		l, err := fp.ReadAt(header, p)
		if l == len(header) {
			// Parse it...
			b := binary.LittleEndian.Uint64(header)
			offsets[uint(b)] = uint64(p) + uint64(len(header))
		} else {
			break // EOF
		}
		if err != nil {
			return nil, err
		}
		p += int64(blockHeaderSize + blockSize)
	}

	return &FileStorageSparse{
		f:           f,
		fp:          fp,
		size:        size,
		blockSize:   blockSize,
		offsets:     offsets,
		currentSize: uint64(len(offsets) * (blockHeaderSize + blockSize)),
		writeLocks:  locks,
	}, nil
}

/**
 * Write a block, or partial block.
 * If the block is not found, the block must be complete (not partial)
 * If the block is not found, and is a complete block, it'll be appended to the storage.
 */
func (i *FileStorageSparse) writeBlock(buffer []byte, b uint, blockOffset int64) error {
	i.offsetsLock.RLock()
	off, ok := i.offsets[b]
	i.offsetsLock.RUnlock()
	if ok {
		// Write to the data where it is...
		_, err := i.fp.WriteAt(buffer, int64(off)+blockOffset)
		return err
	}

	// It needs to be a complete block...
	if blockOffset != 0 || len(buffer) != i.blockSize {
		return errNoBlock
	}

	// Need to append the data to the end of the file...
	i.offsetsLock.Lock()
	defer i.offsetsLock.Unlock()
	blockHeader := make([]byte, blockHeaderSize)
	binary.LittleEndian.PutUint64(blockHeader, uint64(b))
	i.offsets[b] = i.currentSize + blockHeaderSize
	_, err := i.fp.Seek(int64(i.currentSize), 0) // Go to the end of the file
	if err != nil {
		return err
	}

	i.currentSize += blockHeaderSize + uint64(i.blockSize)
	_, err = i.fp.Write(blockHeader)
	if err != nil {
		return err
	}
	_, err = i.fp.Write(buffer)
	return err
}

/**
 * Read a block, or partial block
 *
 */
func (i *FileStorageSparse) readBlock(buffer []byte, b uint, blockOffset int64) error {
	i.offsetsLock.RLock()
	off, ok := i.offsets[b]
	i.offsetsLock.RUnlock()
	if ok {
		// Read the data where it is...
		_, err := i.fp.ReadAt(buffer, int64(off)+blockOffset)
		return err
	}
	return errors.New("cannot do a partial block write on incomplete block")
}

/**
 * Implementation of ReadAt
 *
 */
func (i *FileStorageSparse) ReadAt(buffer []byte, offset int64) (int, error) {
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

	// Lock and unlock the blocks involved (read lock)
	for b := bStart; b < bEnd; b++ {
		i.writeLocks[b].RLock()
	}
	defer func() {
		for b := bStart; b < bEnd; b++ {
			i.writeLocks[b].RUnlock()
		}
	}()

	errs := make(chan error, bEnd-bStart+1)
	var count uint64

	for blockNo := bStart; blockNo < bEnd; blockNo++ {
		go func(b uint) {
			blockOffset := int64(b) * int64(i.blockSize)
			var err error
			var n int

			if blockOffset >= offset {
				s := blockOffset - offset
				n = int(bufferEnd - s)
				if n > i.blockSize {
					n = i.blockSize
				}
				err = i.readBlock(buffer[s:s+int64(n)], b, 0)
			} else {
				// Partial read at the start
				s := offset - blockOffset
				n = i.blockSize - int(s)
				if n > int(bufferEnd) {
					n = int(bufferEnd)
				}
				err = i.readBlock(buffer[:n], b, s)
			}

			if err == nil {
				atomic.AddUint64(&count, uint64(n))
			}
			errs <- err
		}(blockNo)
	}

	// Wait for all reads to complete, and return any error
	for blockNo := bStart; blockNo < bEnd; blockNo++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
	}

	return int(count), nil
}

/**
 * Implementation of WriteAt
 *
 */
func (i *FileStorageSparse) WriteAt(buffer []byte, offset int64) (int, error) {
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
	count := 0

	// Lock and unlock the blocks involved.
	for b := bStart; b < bEnd; b++ {
		i.writeLocks[b].Lock()
	}
	defer func() {
		for b := bStart; b < bEnd; b++ {
			i.writeLocks[b].Unlock()
		}
	}()

	for b := bStart; b < bEnd; b++ {
		blockOffset := int64(b) * int64(i.blockSize)
		if blockOffset >= offset {
			if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
				// Partial write at the end

				// Try doing the write in place
				dataLen := bufferEnd - (blockOffset - offset)

				err := i.writeBlock(buffer[blockOffset-offset:bufferEnd], b, 0)
				if err == errNoBlock {
					// The block doesn't exist yet
					blockBuffer := make([]byte, i.blockSize)
					var err error

					// If the write doesn't extend to the end of the storage size, we need to do a read first.
					if blockOffset+dataLen < int64(i.size) {
						err = i.readBlock(blockBuffer, b, 0)
					}
					if err != nil {
						return 0, err
					}
					// Merge the data in, and write it back...
					count += copy(blockBuffer, buffer[blockOffset-offset:bufferEnd])
					err = i.writeBlock(blockBuffer, b, 0)
					if err != nil {
						return 0, err
					}
				} else if err != nil {
					return 0, err
				} else if err == nil {
					count += int(dataLen)
				}
			} else {
				// Complete write in the middle - no need for a read.
				s := blockOffset - offset
				e := s + int64(i.blockSize)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				err := i.writeBlock(buffer[s:e], b, 0)
				if err != nil {
					return 0, err
				}
				count += i.blockSize
			}
		} else {
			dataStart := offset - blockOffset
			be := i.blockSize - int(dataStart)
			if be > len(buffer) {
				be = len(buffer)
			}
			// First try doing the write without a pre-read (If the block already exists)
			err := i.writeBlock(buffer[:be], b, dataStart)
			if err == errNoBlock {
				// We need to read, merge and write a complete block.

				// Partial write at the start
				blockBuffer := make([]byte, i.blockSize)
				err := i.readBlock(blockBuffer, b, 0)
				if err != nil {
					return 0, err
				}
				// Merge the data in, and write it back...
				count += copy(blockBuffer[offset-blockOffset:], buffer[:bufferEnd])
				err = i.writeBlock(blockBuffer, b, 0)
				if err != nil {
					return 0, nil
				}
			} else if err != nil {
				return 0, nil
			} else if err == nil {
				count += be
			}
		}
	}
	return count, nil
}

func (i *FileStorageSparse) Close() error {
	i.wg.Wait() // Wait for any pending ops to complete
	return i.fp.Close()
}

func (i *FileStorageSparse) Flush() error {
	return i.fp.Sync()
}

func (i *FileStorageSparse) Size() uint64 {
	return i.size
}

func (i *FileStorageSparse) CancelWrites(_ int64, _ int64) {}
