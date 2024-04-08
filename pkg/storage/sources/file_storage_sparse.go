package sources

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

/**
 * Simple sparse file storage provider
 *
 * - Reads return error if no data has been written for a block yet.
 * - Partial block reads supported as long as the blocks exist.
 * - Partial block writes supported as long as the blocks exist (Have already been written to completely).
 *
 */
type FileStorageSparse struct {
	f           string
	fp          *os.File
	size        uint64
	blockSize   int
	offsets     map[uint]uint64
	writeLock   sync.RWMutex
	currentSize uint64
}

func NewFileStorageSparseCreate(f string, size uint64, blockSize int) (*FileStorageSparse, error) {
	fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE, 0666)
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

	header := make([]byte, 8)
	p := int64(0)
	for {
		l, err := fp.ReadAt(header, p)
		if l == len(header) {
			// Parse it...
			b := binary.LittleEndian.Uint64(header)
			offsets[uint(b)] = uint64(p) + uint64(len(header))
		} else {
			break
		}
		if err != nil {
			return nil, err
		}
		p += int64(len(header) + blockSize)
	}
	// Read the offsets file...

	return &FileStorageSparse{
		f:           f,
		fp:          fp,
		size:        size,
		blockSize:   blockSize,
		offsets:     offsets,
		currentSize: uint64(len(offsets) * blockSize),
	}, nil
}

func (i *FileStorageSparse) writeBlock(buffer []byte, b uint) error {
	i.writeLock.RLock()
	off, ok := i.offsets[b]
	if ok {
		// Write to the data where it is...
		_, err := i.fp.WriteAt(buffer, int64(off))
		i.writeLock.RUnlock()
		return err
	}
	i.writeLock.RUnlock()

	// Doing an append on the file, we will lock completely so any ReadAt / WriteAt and offsets access don't happen
	i.writeLock.Lock()
	defer i.writeLock.Unlock()

	// Need to append the data to the end of the file...
	blockHeader := make([]byte, 8)
	binary.LittleEndian.PutUint64(blockHeader, uint64(b))
	i.offsets[b] = i.currentSize + uint64(len(blockHeader))
	i.currentSize += uint64(len(blockHeader)) + uint64(i.blockSize)
	_, err := i.fp.Seek(0, 2) // Go to the end of the file
	if err != nil {
		return err
	}
	_, err = i.fp.Write(blockHeader)
	if err != nil {
		return err
	}
	_, err = i.fp.Write(buffer)
	return err
}

func (i *FileStorageSparse) readBlock(buffer []byte, b uint) error {
	i.writeLock.RLock()
	defer i.writeLock.RUnlock()

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
	count := 0

	// FIXME: We could paralelise these
	for b := b_start; b < b_end; b++ {
		block_offset := int64(b) * int64(i.blockSize)
		if block_offset >= offset {
			if len(buffer[block_offset-offset:buffer_end]) < i.blockSize {
				// Partial read at the end
				block_buffer := make([]byte, i.blockSize)
				err := i.readBlock(block_buffer, b)
				if err == nil {
					count += copy(buffer[block_offset-offset:buffer_end], block_buffer)
				} else {
					return 0, err
				}
			} else {
				s := block_offset - offset
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
			block_buffer := make([]byte, i.blockSize)
			err := i.readBlock(block_buffer, b)
			if err == nil {
				count += copy(buffer[:buffer_end], block_buffer[offset-block_offset:])
			} else {
				return 0, err
			}
		}
	}

	return count, nil
}

func (i *FileStorageSparse) WriteAt(buffer []byte, offset int64) (int, error) {
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
	count := 0

	for b := b_start; b < b_end; b++ {
		block_offset := int64(b) * int64(i.blockSize)
		if block_offset >= offset {
			if len(buffer[block_offset-offset:buffer_end]) < i.blockSize {
				// Partial write at the end
				block_buffer := make([]byte, i.blockSize)
				var err error
				// IF it's the last block partial, we don't need to do a read. It's complete already
				if block_offset+int64(i.blockSize) > int64(i.size) {
					// We don't need to read the last block here.
				} else {
					err = i.readBlock(block_buffer, b)
				}
				if err != nil {
					return 0, err
				} else {
					// Merge the data in, and write it back...
					count += copy(block_buffer, buffer[block_offset-offset:buffer_end])
					err := i.writeBlock(block_buffer, b)
					if err != nil {
						return 0, nil
					}
				}
			} else {
				s := block_offset - offset
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
			block_buffer := make([]byte, i.blockSize)
			err := i.readBlock(block_buffer, b)
			if err != nil {
				return 0, err
			} else {
				// Merge the data in, and write it back...
				count += copy(block_buffer[offset-block_offset:], buffer[:buffer_end])
				err := i.writeBlock(block_buffer, b)
				if err != nil {
					return 0, nil
				}
			}
		}
	}
	return count, nil
}

func (i *FileStorageSparse) Close() error {
	return i.fp.Close()
}

func (i *FileStorageSparse) Flush() error {
	i.fp.Sync()
	return nil
}

func (i *FileStorageSparse) Size() uint64 {
	return uint64(i.size)
}
