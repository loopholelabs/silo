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
func (i *DummyTracker) SendEvent(event_type storage.EventType, event_data storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendEvent(event_type, event_data)
	return append(data, storage.SendEvent(i.prov, event_type, event_data)...)
}

func NewDummyTracker(prov storage.StorageProvider, block_size int) *DummyTracker {
	num_blocks := (int(prov.Size()) + block_size - 1) / block_size
	l := &DummyTracker{
		prov: prov,
		bf:   util.NewBitfield(num_blocks),
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
