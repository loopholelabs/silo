package sources

import (
	"io"
	"os"
	"sync"
)

/**
 * Simple fixed size file storage provider
 *
 */
type FileStorage struct {
	fp   *os.File
	size int64
	wg   sync.WaitGroup
}

func NewFileStorage(f string, size int64) (*FileStorage, error) {
	fp, err := os.OpenFile(f, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &FileStorage{
		fp:   fp,
		size: size,
	}, nil
}

func NewFileStorageCreate(f string, size int64) (*FileStorage, error) {
	fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	err = fp.Truncate(size)
	if err != nil {
		return nil, err
	}
	return &FileStorage{
		fp:   fp,
		size: size,
	}, nil
}

func (i *FileStorage) ReadAt(buffer []byte, offset int64) (int, error) {
	i.wg.Add(1)
	defer i.wg.Done()
	// We don't want to return EOF
	n, err := i.fp.ReadAt(buffer, offset)
	if n < len(buffer) && err == io.EOF {
		err = nil
	}
	return n, err
}

func (i *FileStorage) WriteAt(buffer []byte, offset int64) (int, error) {
	i.wg.Add(1)
	defer i.wg.Done()
	data := buffer
	if offset > i.size {
		return 0, io.EOF
	}
	if offset+int64(len(data)) > i.size {
		data = data[:(i.size - offset)]
	}
	return i.fp.WriteAt(data, offset)
}

func (i *FileStorage) Flush() error {
	return i.fp.Sync()
}

func (i *FileStorage) Size() uint64 {
	return uint64(i.size)
}

func (i *FileStorage) Close() error {
	i.wg.Wait()
	return i.fp.Close()
}

func (i *FileStorage) CancelWrites(offset int64, length int64) {}
