package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/bitfield"
)

type DummyTracker struct {
	storage.ProviderWithEvents
	prov storage.Provider
	bf   *bitfield.Bitfield
}

// Relay events to embedded StorageProvider
func (i *DummyTracker) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewDummyTracker(prov storage.Provider, blockSize int) *DummyTracker {
	numBlocks := (int(prov.Size()) + blockSize - 1) / blockSize
	l := &DummyTracker{
		prov: prov,
		bf:   bitfield.NewBitfield(numBlocks),
	}
	return l
}

func (i *DummyTracker) Sync() *bitfield.Bitfield {
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
