package storage

import "io"

type StorageError int

const StorageError_SUCCESS = StorageError(0)
const StorageError_ERROR = StorageError(1)

type StorageProvider interface {
	io.ReaderAt
	io.WriterAt
	Size() uint64
	Flush() error
}

type ExposedStorage interface {
	Handle(prov StorageProvider) error
	WaitReady() error
	Shutdown() error
}
