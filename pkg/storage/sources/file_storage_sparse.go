package sources

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

/**
 * Simple sparse file storage provider
 *
 * - Reads default to ZERO if no data has been written for a block.
 * - Partial block reads supported.
 * - Only complete block writes count. (Partial blocks are discarded).
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

	// Read the offsets file...
	offsets := make(map[uint]uint64)

	data, err := os.ReadFile(fmt.Sprintf("%s.offsets", f))
	if err != nil {
		return nil, err
	}

	// Parse the data
	p := 0
	for {
		if p >= len(data) {
			break
		}
		block := binary.LittleEndian.Uint64(data[p:])
		offset := binary.LittleEndian.Uint64(data[p+8:])
		offsets[uint(block)] = offset
		p += 16
	}

	return &FileStorageSparse{
		f:           f,
		fp:          fp,
		size:        size,
		blockSize:   blockSize,
		offsets:     offsets,
		currentSize: uint64(len(offsets) * blockSize),
	}, nil
}

func (i *FileStorageSparse) writeOffsetsFile() error {
	data := make([]byte, len(i.offsets)*16)
	p := 0
	for block, offset := range i.offsets {
		binary.LittleEndian.PutUint64(data[p:], uint64(block))
		binary.LittleEndian.PutUint64(data[p+8:], uint64(offset))
		p += 16
	}
	return os.WriteFile(fmt.Sprintf("%s.offsets", i.f), data, 0666)
}

func (i *FileStorageSparse) writeBlock(buffer []byte, b uint) error {
	i.writeLock.Lock()
	defer i.writeLock.Unlock()

	off, ok := i.offsets[b]
	if ok {
		// Write to the data where it is...
		_, err := i.fp.WriteAt(buffer, int64(off))
		return err
	} else {
		// Need to append the data to the end of the file...
		i.offsets[b] = i.currentSize
		i.currentSize += uint64(i.blockSize)
		_, err := i.fp.Seek(0, 2) // Go to the end
		if err != nil {
			return err
		}
		_, err = i.fp.Write(buffer)
		if err != nil {
			return err
		}
	}
	return nil
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
		// Assume zeros
		for i := range buffer {
			buffer[i] = 0
		}
		return nil
		// panic("read before write on FileStorageSparse")
	}
}

func (i *FileStorageSparse) ReadAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.blockSize))
	b_end := uint((end-1)/uint64(i.blockSize)) + 1

	// Only do complete blocks...
	for b := b_start; b < b_end; b++ {
		block_offset := int64(b) * int64(i.blockSize)
		if block_offset >= offset {
			if len(buffer[block_offset-offset:]) < i.blockSize {
				// Partial read at the end
				block_buffer := make([]byte, i.blockSize)
				err := i.readBlock(block_buffer, b)
				if err == nil {
					copy(buffer[block_offset-offset:], block_buffer)
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
			}
		} else {
			// Partial read at the start
			block_buffer := make([]byte, i.blockSize)
			err := i.readBlock(block_buffer, b)
			if err == nil {
				copy(buffer, block_buffer[offset-block_offset:])
			} else {
				return 0, err
			}
		}
	}

	return len(buffer), nil
}

func (i *FileStorageSparse) WriteAt(buffer []byte, offset int64) (int, error) {

	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.blockSize))
	b_end := uint((end-1)/uint64(i.blockSize)) + 1

	// Only do complete blocks...
	for b := b_start; b < b_end; b++ {
		block_offset := int64(b) * int64(i.blockSize)
		if block_offset >= offset {
			if len(buffer[block_offset-offset:]) < i.blockSize {
				// Partial write at the end
				// FIXME: For now, we IGNORE Partial blocks.
			} else {
				s := block_offset - offset
				e := s + int64(i.blockSize)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				i.writeBlock(buffer[s:e], b)
			}
		} else {
			// Partial write at the start
			// FIXME: For now, we IGNORE Partial blocks.
		}
	}

	return len(buffer), nil
}

func (i *FileStorageSparse) Close() error {
	// Write out an offsets file for the offset information...
	i.writeOffsetsFile()
	// TODO: Check for error
	return i.fp.Close()
}

func (i *FileStorageSparse) Flush() error {
	i.writeOffsetsFile()
	// TODO: Check for error
	i.fp.Sync()
	return nil
}

func (i *FileStorageSparse) Size() uint64 {
	return uint64(i.size)
}
