package modules

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

type Logger struct {
	storage.StorageProviderWithEvents
	prov    storage.StorageProvider
	prefix  string
	enabled atomic.Bool
}

// Relay events to embedded StorageProvider
func (i *Logger) SendSiloEvent(event_type storage.EventType, event_data storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendSiloEvent(event_type, event_data)
	return append(data, storage.SendSiloEvent(i.prov, event_type, event_data)...)
}

func NewLogger(prov storage.StorageProvider, prefix string) *Logger {
	l := &Logger{
		prov:   prov,
		prefix: prefix,
	}
	l.enabled.Store(true)
	return l
}

func (i *Logger) Disable() {
	i.enabled.Store(false)
}

func (i *Logger) Enable() {
	i.enabled.Store(true)
}

func (i *Logger) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := i.prov.ReadAt(buffer, offset)
	if i.enabled.Load() {
		fmt.Printf("%v: %s ReadAt(%d, offset=%d) -> %d, %v\n", time.Now(), i.prefix, len(buffer), offset, n, err)
	}
	return n, err
}

func (i *Logger) WriteAt(buffer []byte, offset int64) (int, error) {
	n, err := i.prov.WriteAt(buffer, offset)
	if i.enabled.Load() {
		fmt.Printf("%v: %s WriteAt(%d, offset=%d) -> %d, %v\n", time.Now(), i.prefix, len(buffer), offset, n, err)
	}
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

func (i *Logger) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
	// TODO: Implement
}
