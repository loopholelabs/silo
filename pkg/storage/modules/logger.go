package modules

import (
	"sync/atomic"

	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
)

type Logger struct {
	storage.ProviderWithEvents
	prov    storage.Provider
	prefix  string
	log     types.Logger
	enabled atomic.Bool
}

// Relay events to embedded StorageProvider
func (i *Logger) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewLogger(prov storage.Provider, prefix string, log types.Logger) *Logger {
	l := &Logger{
		prov:   prov,
		log:    log,
		prefix: prefix,
	}
	l.enabled.Store(true)
	return l
}

func (i *Logger) Disable() {
	if i.enabled.Load() && i.log != nil {
		i.log.Debug().Str("device", i.prefix).Msg("logging disabled")
	}
	i.enabled.Store(false)
}

func (i *Logger) Enable() {
	i.enabled.Store(true)
	if i.enabled.Load() && i.log != nil {
		i.log.Debug().Str("device", i.prefix).Msg("logging enabled")
	}
}

func (i *Logger) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := i.prov.ReadAt(buffer, offset)
	if i.enabled.Load() && i.log != nil {
		i.log.Debug().
			Str("device", i.prefix).
			Int("length", len(buffer)).
			Int64("offset", offset).
			Int("n", n).
			Err(err).
			Msg("ReadAt")
	}
	return n, err
}

func (i *Logger) WriteAt(buffer []byte, offset int64) (int, error) {
	n, err := i.prov.WriteAt(buffer, offset)
	if i.enabled.Load() && i.log != nil {
		i.log.Debug().
			Str("device", i.prefix).
			Int("length", len(buffer)).
			Int64("offset", offset).
			Int("n", n).
			Err(err).
			Msg("WriteAt")
	}
	return n, err
}

func (i *Logger) Flush() error {
	err := i.prov.Flush()
	if i.enabled.Load() && i.log != nil {
		i.log.Debug().
			Str("device", i.prefix).
			Err(err).
			Msg("Flush")
	}
	return err
}

func (i *Logger) Size() uint64 {
	return i.prov.Size()
}

func (i *Logger) Close() error {
	err := i.prov.Close()
	if i.enabled.Load() && i.log != nil {
		i.log.Debug().
			Str("device", i.prefix).
			Err(err).
			Msg("Close")
	}
	return err
}

func (i *Logger) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
	if i.enabled.Load() && i.log != nil {
		i.log.Debug().
			Str("device", i.prefix).
			Int64("offset", offset).
			Int64("length", length).
			Msg("CancelWrites")
	}
}
