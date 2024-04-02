package modules

import (
	"fmt"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

type Logger struct {
	prov   storage.StorageProvider
	prefix string
}

func NewLogger(prov storage.StorageProvider, prefix string) *Logger {
	return &Logger{
		prov:   prov,
		prefix: prefix,
	}
}

func (i *Logger) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := i.prov.ReadAt(buffer, offset)
	fmt.Printf("%v: %s ReadAt(%d, offset=%d) -> %d, %v\n", time.Now(), i.prefix, len(buffer), offset, n, err)
	return n, err
}

func (i *Logger) WriteAt(buffer []byte, offset int64) (int, error) {
	n, err := i.prov.WriteAt(buffer, offset)
	fmt.Printf("%v: %s WriteAt(%d, offset=%d) -> %d, %v\n", time.Now(), i.prefix, len(buffer), offset, n, err)
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
