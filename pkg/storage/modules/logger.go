package modules

import (
	"fmt"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

type Logger struct {
	prov storage.StorageProvider
}

func NewLogger(prov storage.StorageProvider) *Logger {
	return &Logger{
		prov: prov,
	}
}

func (i *Logger) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := i.prov.ReadAt(buffer, offset)
	fmt.Printf("%v: ReadAt(%d, offset=%d) -> %d, %v\n", time.Now(), len(buffer), offset, n, err)
	return n, err
}

func (i *Logger) WriteAt(buffer []byte, offset int64) (int, error) {
	n, err := i.prov.WriteAt(buffer, offset)
	fmt.Printf("%v: WriteAt(%d, offset=%d) -> %d, %v\n", time.Now(), len(buffer), offset, n, err)
	return n, err
}

func (i *Logger) Flush() error {
	return i.prov.Flush()
}

func (i *Logger) Size() uint64 {
	return i.prov.Size()
}

func (i *Logger) Close() error {
	return i.prov.Close()
}
