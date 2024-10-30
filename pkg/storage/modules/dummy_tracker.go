package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type DummyTracker struct {
	storage.StorageProviderWithEvents
	prov storage.StorageProvider
	bf   *util.Bitfield
}

// Relay events to embedded StorageProvider
func (i *DummyTracker) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewDummyTracker(prov storage.StorageProvider, blockSize int) *DummyTracker {
	numBlocks := (int(prov.Size()) + blockSize - 1) / blockSize
	l := &DummyTracker{
		prov: prov,
		bf:   util.NewBitfield(numBlocks),
	}
	return l
}

func (i *DummyTracker) Sync() *util.Bitfield {
	return i.bf
}

func (i *DummyTracker) ReadAt(buffer []byte, offset int64) (int, error) {
	return i.prov.ReadAt(buffer, offset)
}

func (i *DummyTracker) WriteAt(buffer []byte, offset int64) (int, error) {
	return i.prov.WriteAt(buffer, offset)
}

func (i *DummyTracker) Flush() error {
	return i.prov.Flush()
}

func (i *DummyTracker) Size() uint64 {
	return i.prov.Size()
}

func (i *DummyTracker) Close() error {
	return i.prov.Close()
}

func (i *DummyTracker) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
