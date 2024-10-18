package sources

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"

	"github.com/google/uuid"
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
	storage.StorageProviderLifecycleState
	uuid         uuid.UUID
	f            string
	fp           *os.File
	size         uint64
	block_size   int
	offsets      map[uint]uint64
	write_lock   sync.Mutex
	current_size uint64
	wg           sync.WaitGroup
}

func NewFileStorageSparseCreate(f string, size uint64, blockSize int) (*FileStorageSparse, error) {
	fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	return &FileStorageSparse{
		uuid:         uuid.New(),
		f:            f,
		fp:           fp,
		size:         size,
		block_size:   blockSize,
		offsets:      make(map[uint]uint64),
		current_size: 0,
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
		uuid:         uuid.New(),
		f:            f,
		fp:           fp,
		size:         size,
		block_size:   blockSize,
		offsets:      offsets,
		current_size: uint64(len(offsets) * (BLOCK_HEADER_SIZE + blockSize)),
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
	i.offsets[b] = i.current_size + BLOCK_HEADER_SIZE
	_, err := i.fp.Seek(int64(i.current_size), 0) // Go to the end of the file
	if err != nil {
		return err
	}

	i.current_size += BLOCK_HEADER_SIZE + uint64(i.block_size)
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

func (i *FileStorageSparse) UUID() []uuid.UUID {
	return []uuid.UUID{i.uuid}
}

func (i *FileStorageSparse) ReadAt(buffer []byte, offset int64) (int, error) {
	i.wg.Add(1)
	defer i.wg.Done()
	// FIXME: overkill lock
	i.write_lock.Lock()
	defer i.write_lock.Unlock()

	buffer_end := int64(len(buffer))
	if offset+int64(len(buffer)) > int64(i.size) {
		// Get rid of any extra data that we can't store...
		buffer_end = int64(i.size) - offset
	}

	end := uint64(offset + buffer_end)
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1
	count := 0

	// FIXME: We should paralelise these
	for b := b_start; b < b_end; b++ {
		block_offset := int64(b) * int64(i.block_size)
		if block_offset >= offset {
			if len(buffer[block_offset-offset:buffer_end]) < i.block_size {
				// Partial read at the end
				block_buffer := make([]byte, i.block_size)
				err := i.readBlock(block_buffer, b)
				if err == nil {
					count += copy(buffer[block_offset-offset:buffer_end], block_buffer)
				} else {
					return 0, err
				}
			} else {
				s := block_offset - offset
				e := s + int64(i.block_size)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				err := i.readBlock(buffer[s:e], b)
				if err != nil {
					return 0, err
				}
				count += i.block_size
			}
		} else {
			// Partial read at the start
			block_buffer := make([]byte, i.block_size)
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
	i.wg.Add(1)
	defer i.wg.Done()

	i.write_lock.Lock()
	defer i.write_lock.Unlock()

	buffer_end := int64(len(buffer))
	if offset+int64(len(buffer)) > int64(i.size) {
		// Get rid of any extra data that we can't store...
		buffer_end = int64(i.size) - offset
	}

	end := uint64(offset + buffer_end)
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1
	count := 0

	for b := b_start; b < b_end; b++ {
		block_offset := int64(b) * int64(i.block_size)
		if block_offset >= offset {
			if len(buffer[block_offset-offset:buffer_end]) < i.block_size {
				// Partial write at the end
				block_buffer := make([]byte, i.block_size)
				var err error
				// IF it's the last block partial, we don't need to do a read. It's complete already
				if block_offset+int64(i.block_size) > int64(i.size) {
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
				e := s + int64(i.block_size)
				if e > int64(len(buffer)) {
					e = int64(len(buffer))
				}
				err := i.writeBlock(buffer[s:e], b)
				if err != nil {
					return 0, err
				}
				count += i.block_size
			}
		} else {
			// Partial write at the start
			block_buffer := make([]byte, i.block_size)
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
