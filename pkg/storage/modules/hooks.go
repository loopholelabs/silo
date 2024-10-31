package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
)

type Hooks struct {
	storage.ProviderWithEvents
	prov      storage.Provider
	PreRead   func(_ []byte, _ int64) (bool, int, error)
	PostRead  func(_ []byte, _ int64, _ int, _ error) (int, error)
	PreWrite  func(_ []byte, _ int64) (bool, int, error)
	PostWrite func(_ []byte, _ int64, _ int, _ error) (int, error)
	PreClose  func() error
	PostClose func(_ error) error
}

// Relay events to embedded StorageProvider
func (i *Hooks) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewHooks(prov storage.Provider) *Hooks {
	return &Hooks{
		prov: prov,
		PreRead: func(_ []byte, _ int64) (bool, int, error) {
			return false, 0, nil
		},
		PreWrite: func(_ []byte, _ int64) (bool, int, error) {
			return false, 0, nil
		},
		PostRead: func(_ []byte, _ int64, n int, err error) (int, error) {
			return n, err
		},
		PostWrite: func(_ []byte, _ int64, n int, err error) (int, error) {
			return n, err
		},
		PreClose: func() error {
			return nil
		},
		PostClose: func(err error) error {
			return err
		},
	}
}

func (i *Hooks) ReadAt(buffer []byte, offset int64) (int, error) {
	ok, n, err := i.PreRead(buffer, offset)
	if ok {
		return n, err
	}
	n, err = i.prov.ReadAt(buffer, offset)
	n, err = i.PostRead(buffer, offset, n, err)
	return n, err
}

func (i *Hooks) WriteAt(buffer []byte, offset int64) (int, error) {
	ok, n, err := i.PreWrite(buffer, offset)
	if ok {
		return n, err
	}
	n, err = i.prov.WriteAt(buffer, offset)
	n, err = i.PostWrite(buffer, offset, n, err)
	return n, err
}

func (i *Hooks) Flush() error {
	return i.prov.Flush()
}

func (i *Hooks) Size() uint64 {
	return i.prov.Size()
}

func (i *Hooks) Close() error {
	err := i.PreClose()
	if err != nil {
		return err
	}
	err = i.prov.Close()
	return i.PostClose(err)
}

func (i *Hooks) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
