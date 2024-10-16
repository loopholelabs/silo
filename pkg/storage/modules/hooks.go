package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
)

type Hooks struct {
	prov       storage.StorageProvider
	Pre_read   func(buffer []byte, offset int64) (bool, int, error)
	Post_read  func(buffer []byte, offset int64, n int, err error) (int, error)
	Pre_write  func(buffer []byte, offset int64) (bool, int, error)
	Post_write func(buffer []byte, offset int64, n int, err error) (int, error)
}

func NewHooks(prov storage.StorageProvider) *Hooks {
	return &Hooks{
		prov: prov,
		Pre_read: func(buffer []byte, offset int64) (bool, int, error) {
			return false, 0, nil
		},
		Pre_write: func(buffer []byte, offset int64) (bool, int, error) {
			return false, 0, nil
		},
		Post_read: func(buffer []byte, offset int64, n int, err error) (int, error) {
			return n, err
		},
		Post_write: func(buffer []byte, offset int64, n int, err error) (int, error) {
			return n, err
		},
	}
}

func (i *Hooks) ReadAt(buffer []byte, offset int64) (int, error) {
	ok, n, err := i.Pre_read(buffer, offset)
	if ok {
		return n, err
	}
	n, err = i.prov.ReadAt(buffer, offset)
	n, err = i.Post_read(buffer, offset, n, err)
	return n, err
}

func (i *Hooks) WriteAt(buffer []byte, offset int64) (int, error) {
	ok, n, err := i.Pre_write(buffer, offset)
	if ok {
		return n, err
	}
	n, err = i.prov.WriteAt(buffer, offset)
	n, err = i.Post_write(buffer, offset, n, err)
	return n, err
}

func (i *Hooks) Flush() error {
	return i.prov.Flush()
}

func (i *Hooks) Size() uint64 {
	return i.prov.Size()
}

func (i *Hooks) Close() error {
	return i.prov.Close()
}

func (i *Hooks) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
