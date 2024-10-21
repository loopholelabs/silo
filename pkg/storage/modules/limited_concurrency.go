package modules

import (
	"github.com/google/uuid"
	"github.com/loopholelabs/silo/pkg/storage"
)

type LimitedConcurrency struct {
	storage.StorageProviderWithEvents
	prov              storage.StorageProvider
	concurrent_reads  chan bool
	concurrent_writes chan bool
}

func (i *LimitedConcurrency) SendEvent(event_type storage.EventType, event_data storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendEvent(event_type, event_data)
	return append(data, storage.SendEvent(i.prov, event_type, event_data)...)
}

func NewLimitedConcurrency(prov storage.StorageProvider, max_reads int, max_writes int) *LimitedConcurrency {
	return &LimitedConcurrency{
		prov:              prov,
		concurrent_reads:  make(chan bool, max_reads),
		concurrent_writes: make(chan bool, max_writes),
	}
}

func (i *LimitedConcurrency) UUID() []uuid.UUID {
	return i.prov.UUID()
}

func (i *LimitedConcurrency) ReadAt(buffer []byte, offset int64) (int, error) {
	if cap(i.concurrent_reads) == 0 {
		return i.prov.ReadAt(buffer, offset)
	}
	i.concurrent_reads <- true
	defer func() {
		<-i.concurrent_reads
	}()
	return i.prov.ReadAt(buffer, offset)
}

func (i *LimitedConcurrency) WriteAt(buffer []byte, offset int64) (int, error) {
	if cap(i.concurrent_writes) == 0 {
		return i.prov.WriteAt(buffer, offset)
	}
	i.concurrent_writes <- true
	defer func() {
		<-i.concurrent_writes
	}()
	return i.prov.WriteAt(buffer, offset)
}

func (i *LimitedConcurrency) Flush() error {
	return i.prov.Flush()
}

func (i *LimitedConcurrency) Size() uint64 {
	return i.prov.Size()
}

func (i *LimitedConcurrency) Close() error {
	return i.prov.Close()
}

func (i *LimitedConcurrency) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
