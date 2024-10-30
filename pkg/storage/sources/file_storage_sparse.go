package sources

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

const BLOCK_HEADER_SIZE = 8

/**
 * Simple sparse file storage provider
 *
 * - Reads return error if no data has been written for a block yet.
 * - Partial block reads supported as long as the blocks exist.
 * - Partial block writes supported as long as the blocks exist (Have already been written to completely).
 *
 */
type FileStorageSparse struct {
	storage.StorageProviderWithEvents
	f           string
	fp          *os.File
	size        uint64
	blockSize   int
	offsets     map[uint]uint64
	writeLock   sync.Mutex
	currentSize uint64
	wg          sync.WaitGroup
}

func NewFileStorageSparseCreate(f string, size uint64, blockSize int) (*FileStorageSparse, error) {
	fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	return &FileStorageSparse{
		f:           f,
		fp:          fp,
		size:        size,
		blockSize:   blockSize,
		offsets:     make(map[uint]uint64),
		currentSize: 0,
	}, nil
}

func NewFileStorageSparse(f string, size uint64, blockSize int) (*FileStorageSparse, error) {
	fp, err := os.OpenFile(f, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// Scan through the file and get the offsets...
	offsets := make(map[uint]uint64)

	header := make([]byte, BLOCK_HEADER_SIZE)
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
		p += int64(BLOCK_HEADER_SIZE + blockSize)
	}

	return &FileStorageSparse{
		f:           f,
		fp:          fp,
		size:        size,
		blockSize:   blockSize,
		offsets:     offsets,
		currentSize: uint64(len(offsets) * (BLOCK_HEADER_SIZE + blockSize)),
	}, nil
}

func (i *FileStorageSparse) writeBlock(buffer []byte, b uint) error {
	off, ok := i.offsets[b]
	if ok {
		// Write to the data where it is...
		_, err := i.fp.WriteAt(buffer, int64(off))
		return err
	}

	// Need to append the data to the end of the file...
	blockHeader := make([]byte, BLOCK_HEADER_SIZE)
	binary.LittleEndian.PutUint64(blockHeader, uint64(b))
	i.offsets[b] = i.currentSize + BLOCK_HEADER_SIZE
	_, err := i.fp.Seek(int64(i.currentSize), 0) // Go to the end of the file
	if err != nil {
		return err
	}

	i.currentSize += BLOCK_HEADER_SIZE + uint64(i.blockSize)
	_, err = i.fp.Write(blockHeader)
	if err != nil {
		return err
	}
	_, err = i.fp.Write(buffer)
	return err
}

func (i *FileStorageSparse) readBlock(buffer []byte, b uint) error {
	off, ok := i.offsets[b]
	if ok {
		// Read the data where it is...
		_, err := i.fp.ReadAt(buffer, int64(off))
		return err
	} else {
		return errors.New("cannot do a partial block write on incomplete block")
	}
}

func (i *FileStorageSparse) ReadAt(buffer []byte, offset int64) (int, error) {
	i.wg.Add(1)
	defer i.wg.Done()
	// FIXME: overkill lock
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
	count := 0

	// FIXME: We should paralelise these
	for b := bStart; b < bEnd; b++ {
		blockOffset := int64(b) * int64(i.blockSize)
		if blockOffset >= offset {
			if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
				// Partial read at the end
				blockBuffer := make([]byte, i.blockSize)
				err := i.readBlock(blockBuffer, b)
				if err == nil {
					count += copy(buffer[blockOffset-offset:bufferEnd], blockBuffer)
				} else {
					return 0, err
				}
			} else {
				s := blockOffset - offset
				e := s + int64(i.blockSize)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				err := i.readBlock(buffer[s:e], b)
				if err != nil {
					return 0, err
				}
				count += i.blockSize
			}
		} else {
			// Partial read at the start
			blockBuffer := make([]byte, i.blockSize)
			err := i.readBlock(blockBuffer, b)
			if err == nil {
				count += copy(buffer[:bufferEnd], blockBuffer[offset-blockOffset:])
			} else {
				return 0, err
			}
		}
	}

	return count, nil
}

func (i *FileStorageSparse) WriteAt(buffer []byte, offset int64) (int, error) {
	i.wg.Add(1)
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
	count := 0

	for b := bStart; b < bEnd; b++ {
		blockOffset := int64(b) * int64(i.blockSize)
		if blockOffset >= offset {
			if len(buffer[blockOffset-offset:bufferEnd]) < i.blockSize {
				// Partial write at the end
				blockBuffer := make([]byte, i.blockSize)
				var err error

				dataLen := bufferEnd - (blockOffset - offset)

				// If the write doesn't extend to the end of the storage size, we need to do a read first.
				if blockOffset+dataLen < int64(i.size) {
					err = i.readBlock(blockBuffer, b)
				}
				if err != nil {
					return 0, err
				} else {
					// Merge the data in, and write it back...
					count += copy(blockBuffer, buffer[blockOffset-offset:bufferEnd])
					err := i.writeBlock(blockBuffer, b)
					if err != nil {
						return 0, nil
					}
				}
			} else {
				s := blockOffset - offset
				e := s + int64(i.blockSize)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				err := i.writeBlock(buffer[s:e], b)
				if err != nil {
					return 0, err
				}
				count += i.blockSize
			}
		} else {
			// Partial write at the start
			blockBuffer := make([]byte, i.blockSize)
			err := i.readBlock(blockBuffer, b)
			if err != nil {
				return 0, err
			} else {
				// Merge the data in, and write it back...
				count += copy(blockBuffer[offset-blockOffset:], buffer[:bufferEnd])
				err := i.writeBlock(blockBuffer, b)
				if err != nil {
					return 0, nil
				}
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
	return uint64(i.size)
}

func (i *FileStorageSparse) CancelWrites(offset int64, length int64) {}
