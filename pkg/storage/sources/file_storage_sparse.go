package sources

import (
	"os"
	"sync"
)

/**
 * Simple sparse file storage provider
 *
 */
type FileStorageSparse struct {
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
		fp:          fp,
		size:        size,
		blockSize:   blockSize,
		offsets:     make(map[uint]uint64),
		currentSize: 0,
	}, nil
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
				// FIXME: For now, we IGNORE Partial blocks.
			} else {
				s := block_offset - offset
				e := s + int64(i.blockSize)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				i.readBlock(buffer[s:e], b)
			}
		} else {
			// Partial read at the start
			// FIXME: For now, we IGNORE Partial blocks.
		}
	}

	return len(buffer), nil
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
		panic("read before write on FileStorageSparse")
	}
	return nil
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
	return i.fp.Close()
}

func (i *FileStorageSparse) Flush() error {
	i.fp.Sync()
	return nil
}

func (i *FileStorageSparse) Size() uint64 {
	return uint64(i.size)
}
