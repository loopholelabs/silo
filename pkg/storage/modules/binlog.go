package modules

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

type BinLog struct {
	storage.StorageProviderWithEvents
	prov          storage.StorageProvider
	filename      string
	ctime         time.Time
	setCtime      bool
	fp            *os.File
	writeLock     sync.Mutex
	readsEnabled  atomic.Bool
	writesEnabled atomic.Bool
}

// Relay events to embedded StorageProvider
func (i *BinLog) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewBinLog(prov storage.StorageProvider, filename string) (*BinLog, error) {
	fp, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	l := &BinLog{
		prov:     prov,
		filename: filename,
		fp:       fp,
		ctime:    time.Now(),
		setCtime: true,
	}
	// By default, we only log writes.
	l.readsEnabled.Store(false)
	l.writesEnabled.Store(true)
	return l, nil
}

func (i *BinLog) writeLog(data []byte) {
	now := time.Now()
	i.writeLock.Lock()
	defer i.writeLock.Unlock()

	if i.setCtime {
		i.ctime = now
		i.setCtime = false
	}
	// Write a short header, then the packet data
	dt := now.Sub(i.ctime).Nanoseconds()
	header := make([]byte, 12)
	binary.LittleEndian.PutUint64(header, uint64(dt))
	binary.LittleEndian.PutUint32(header[8:], uint32(len(data)))

	// Write to the binlog...
	_, err := i.fp.Write(header)
	if err != nil {
		panic(fmt.Sprintf("Could not write to binlog %v", err))
	}
	_, err = i.fp.Write(data)
	if err != nil {
		panic(fmt.Sprintf("Could not write to binlog %v", err))
	}
}

func (i *BinLog) SetLogging(reads bool, writes bool) {
	i.readsEnabled.Store(reads)
	i.writesEnabled.Store(writes)
}

func (i *BinLog) ReadAt(buffer []byte, offset int64) (int, error) {
	// Write it to the binlog...
	if i.readsEnabled.Load() {
		b := packets.EncodeReadAt(offset, int32(len(buffer)))
		i.writeLog(b)
	}
	n, err := i.prov.ReadAt(buffer, offset)
	return n, err
}

func (i *BinLog) WriteAt(buffer []byte, offset int64) (int, error) {
	if i.writesEnabled.Load() {
		b := packets.EncodeWriteAt(offset, buffer)
		i.writeLog(b)
	}
	n, err := i.prov.WriteAt(buffer, offset)
	return n, err
}

func (i *BinLog) Flush() error {
	return i.prov.Flush()
}

func (i *BinLog) Size() uint64 {
	return i.prov.Size()
}

func (i *BinLog) Close() error {
	i.fp.Close() // Close the binlog.
	return i.prov.Close()
}

func (i *BinLog) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
