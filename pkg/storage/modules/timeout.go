package modules

import (
	"errors"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

var (
	ErrTimeout = errors.New("request timeout")
)

/**
 * Add timeout functionality
 *
 */
type Timeout struct {
	storage.ProviderWithEvents
	prov         storage.Provider
	timeoutRead  time.Duration
	timeoutWrite time.Duration
	timeoutClose time.Duration
	timeoutFlush time.Duration
}

func NewTimeout(prov storage.Provider, timeoutRead time.Duration, timeoutWrite time.Duration, timeoutClose time.Duration, timeoutFlush time.Duration) *Timeout {
	return &Timeout{
		prov:         prov,
		timeoutRead:  timeoutRead,
		timeoutWrite: timeoutWrite,
		timeoutClose: timeoutClose,
		timeoutFlush: timeoutFlush,
	}
}

// Relay events to embedded StorageProvider
func (i *Timeout) SendSiloEvent(event_type storage.EventType, event_data storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(event_type, event_data)
	return append(data, storage.SendSiloEvent(i.prov, event_type, event_data)...)
}

type readWriteResult struct {
	n   int
	err error
}

func (i *Timeout) ReadAt(buffer []byte, offset int64) (int, error) {
	result := make(chan readWriteResult, 1)
	go func() {
		n, err := i.prov.ReadAt(buffer, offset)
		result <- readWriteResult{n: n, err: err}
	}()

	select {
	case <-time.After(i.timeoutRead):
		return 0, ErrTimeout
	case result := <-result:
		return result.n, result.err
	}
}

func (i *Timeout) WriteAt(buffer []byte, offset int64) (int, error) {
	result := make(chan readWriteResult, 1)
	go func() {
		n, err := i.prov.WriteAt(buffer, offset)
		result <- readWriteResult{n: n, err: err}
	}()

	select {
	case <-time.After(i.timeoutWrite):
		return 0, ErrTimeout
	case result := <-result:
		return result.n, result.err
	}
}

func (i *Timeout) Flush() error {
	result := make(chan error, 1)
	go func() {
		result <- i.prov.Flush()
	}()

	select {
	case <-time.After(i.timeoutFlush):
		return ErrTimeout
	case result := <-result:
		return result
	}
}

func (i *Timeout) Size() uint64 {
	return i.prov.Size()
}

func (i *Timeout) Close() error {
	result := make(chan error, 1)
	go func() {
		result <- i.prov.Close()
	}()

	select {
	case <-time.After(i.timeoutClose):
		return ErrTimeout
	case result := <-result:
		return result
	}
}

func (i *Timeout) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
