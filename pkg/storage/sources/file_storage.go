package sources

import "os"

/**
 * Simple fixed size file storage provider
 *
 */
type FileStorage struct {
	fp   *os.File
	size int64
}

func NewFileStorage(f string, size int64) (*FileStorage, error) {
	fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	fp.Truncate(size)
	return &FileStorage{
		fp:   fp,
		size: size,
	}, nil
}

func (i *FileStorage) Close() error {
	return i.fp.Close()
}

func (i *FileStorage) ReadAt(buffer []byte, offset int64) (int, error) {
	return i.fp.ReadAt(buffer, offset)
}

func (i *FileStorage) WriteAt(buffer []byte, offset int64) (int, error) {
	return i.fp.WriteAt(buffer, offset)
}

func (i *FileStorage) Flush() error {
	i.fp.Sync()
	return nil
}

func (i *FileStorage) Size() uint64 {
	return uint64(i.size)
}
