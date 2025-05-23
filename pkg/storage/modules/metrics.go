package modules

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

var OpSizeBuckets = []int{4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024}

/**
 * Simple metrics filter for reader/writer
 *
 */
type Metrics struct {
	storage.ProviderWithEvents
	prov              storage.Provider
	metricReadOps     uint64
	metricReadBytes   uint64
	metricReadTime    uint64
	metricReadErrors  uint64
	metricWriteOps    uint64
	metricWriteBytes  uint64
	metricWriteTime   uint64
	metricWriteErrors uint64
	metricFlushOps    uint64
	metricFlushTime   uint64
	metricFlushErrors uint64

	metricReadOpsLock  sync.Mutex
	metricReadOpsSize  map[int]uint64
	metricWriteOpsLock sync.Mutex
	metricWriteOpsSize map[int]uint64

	metricCurrentReadOps  int64
	metricCurrentWriteOps int64

	metricWriteOpsConcurrency int64
	metricReadOpsConcurrency  int64
	metricOpsConcurrency      int64
}

type MetricsSnapshot struct {
	ReadOps             uint64
	ReadBytes           uint64
	ReadTime            uint64
	ReadErrors          uint64
	WriteOps            uint64
	WriteBytes          uint64
	WriteTime           uint64
	WriteErrors         uint64
	FlushOps            uint64
	FlushTime           uint64
	FlushErrors         uint64
	ReadOpsSize         map[int]uint64
	WriteOpsSize        map[int]uint64
	CurrentReadOps      uint64
	CurrentWriteOps     uint64
	ReadOpsConcurrency  uint64
	WriteOpsConcurrency uint64
	OpsConcurrency      uint64
}

func NewMetrics(prov storage.Provider) *Metrics {
	readSize := make(map[int]uint64)
	writeSize := make(map[int]uint64)
	for _, v := range OpSizeBuckets {
		readSize[v] = 0
		writeSize[v] = 0
	}
	return &Metrics{
		prov:               prov,
		metricReadOpsSize:  readSize,
		metricWriteOpsSize: writeSize,
	}
}

// Relay events to embedded StorageProvider
func (i *Metrics) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	} else if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.3fs", float64(d)/float64(time.Second))
}

func (i *Metrics) ShowStats(prefix string) {
	readOps := atomic.LoadUint64(&i.metricReadOps)
	readErrors := atomic.LoadUint64(&i.metricReadErrors)
	readTime := atomic.LoadUint64(&i.metricReadTime)
	readAvgTime := uint64(0)
	if readOps-readErrors > 0 {
		readAvgTime = readTime / (readOps - readErrors)
	}
	readAvgTimeF := formatDuration(time.Duration(readAvgTime))
	fmt.Printf("%s: Reads=%d (%d bytes) avg latency %s, %d errors, concurrency=%d (all ops concurrency %d)\n",
		prefix,
		readOps,
		atomic.LoadUint64(&i.metricReadBytes),
		readAvgTimeF,
		readErrors,
		atomic.LoadInt64(&i.metricReadOpsConcurrency),
		atomic.LoadInt64(&i.metricOpsConcurrency),
	)

	writeOps := atomic.LoadUint64(&i.metricWriteOps)
	writeErrors := atomic.LoadUint64(&i.metricWriteErrors)
	writeTime := atomic.LoadUint64(&i.metricWriteTime)
	writeAvgTime := uint64(0)
	if writeOps-writeErrors > 0 {
		writeAvgTime = writeTime / (writeOps - writeErrors)
	}
	writeAvgTimeF := formatDuration(time.Duration(writeAvgTime))
	fmt.Printf("%s: Writes=%d (%d bytes) avg latency %s, %d errors, concurrency=%d\n",
		prefix,
		writeOps,
		atomic.LoadUint64(&i.metricWriteBytes),
		writeAvgTimeF,
		writeErrors,
		atomic.LoadInt64(&i.metricWriteOpsConcurrency),
	)

	flushOps := atomic.LoadUint64(&i.metricFlushOps)
	flushErrors := atomic.LoadUint64(&i.metricFlushErrors)
	flushTime := atomic.LoadUint64(&i.metricFlushTime)
	flushAvgTime := uint64(0)
	if flushOps-flushErrors > 0 {
		flushAvgTime = flushTime / (flushOps - flushErrors)
	}
	flushAvgTimeF := formatDuration(time.Duration(flushAvgTime))

	fmt.Printf("Flushes=%d avg latency %s, %d errors\n",
		flushOps,
		flushAvgTimeF,
		flushErrors,
	)
}

func (i *Metrics) GetMetrics() *MetricsSnapshot {
	ms := &MetricsSnapshot{
		ReadOps:             atomic.LoadUint64(&i.metricReadOps),
		ReadBytes:           atomic.LoadUint64(&i.metricReadBytes),
		ReadTime:            atomic.LoadUint64(&i.metricReadTime),
		ReadErrors:          atomic.LoadUint64(&i.metricReadErrors),
		WriteOps:            atomic.LoadUint64(&i.metricWriteOps),
		WriteBytes:          atomic.LoadUint64(&i.metricWriteBytes),
		WriteTime:           atomic.LoadUint64(&i.metricWriteTime),
		WriteErrors:         atomic.LoadUint64(&i.metricWriteErrors),
		FlushOps:            atomic.LoadUint64(&i.metricFlushOps),
		FlushTime:           atomic.LoadUint64(&i.metricFlushTime),
		FlushErrors:         atomic.LoadUint64(&i.metricFlushErrors),
		CurrentReadOps:      uint64(atomic.LoadInt64(&i.metricCurrentReadOps)),
		CurrentWriteOps:     uint64(atomic.LoadInt64(&i.metricCurrentWriteOps)),
		ReadOpsConcurrency:  uint64(atomic.LoadInt64(&i.metricReadOpsConcurrency)),
		WriteOpsConcurrency: uint64(atomic.LoadInt64(&i.metricWriteOpsConcurrency)),
		OpsConcurrency:      uint64(atomic.LoadInt64(&i.metricOpsConcurrency)),
	}

	readOpsSize := make(map[int]uint64)
	i.metricReadOpsLock.Lock()
	for k, v := range i.metricReadOpsSize {
		readOpsSize[k] = v
	}
	i.metricReadOpsLock.Unlock()
	ms.ReadOpsSize = readOpsSize

	writeOpsSize := make(map[int]uint64)
	i.metricWriteOpsLock.Lock()
	for k, v := range i.metricWriteOpsSize {
		writeOpsSize[k] = v
	}
	i.metricWriteOpsLock.Unlock()
	ms.WriteOpsSize = writeOpsSize

	return ms
}

func (i *Metrics) ResetMetrics() {
	atomic.StoreUint64(&i.metricReadOps, 0)
	atomic.StoreUint64(&i.metricReadBytes, 0)
	atomic.StoreUint64(&i.metricReadTime, 0)
	atomic.StoreUint64(&i.metricReadErrors, 0)
	atomic.StoreUint64(&i.metricWriteOps, 0)
	atomic.StoreUint64(&i.metricWriteBytes, 0)
	atomic.StoreUint64(&i.metricWriteTime, 0)
	atomic.StoreUint64(&i.metricWriteErrors, 0)
	atomic.StoreUint64(&i.metricFlushOps, 0)
	atomic.StoreUint64(&i.metricFlushTime, 0)
	atomic.StoreUint64(&i.metricFlushErrors, 0)
	i.metricReadOpsLock.Lock()
	for k := range i.metricReadOpsSize {
		i.metricReadOpsSize[k] = 0
	}
	i.metricReadOpsLock.Unlock()

	i.metricWriteOpsLock.Lock()
	for k := range i.metricWriteOpsSize {
		i.metricWriteOpsSize[k] = 0
	}
	i.metricWriteOpsLock.Unlock()
}

func (i *Metrics) ReadAt(buffer []byte, offset int64) (int, error) {
	cc := atomic.AddInt64(&i.metricCurrentReadOps, 1)
	defer atomic.AddInt64(&i.metricCurrentReadOps, -1)
	atomic.AddInt64(&i.metricReadOpsConcurrency, cc)
	atomic.AddInt64(&i.metricOpsConcurrency, cc+atomic.LoadInt64(&i.metricCurrentWriteOps))

	i.metricReadOpsLock.Lock()
	num := len(buffer)
	for _, v := range OpSizeBuckets {
		if num <= v {
			i.metricReadOpsSize[v]++
			break
		}
	}
	i.metricReadOpsLock.Unlock()

	atomic.AddUint64(&i.metricReadOps, 1)
	atomic.AddUint64(&i.metricReadBytes, uint64(len(buffer)))
	ctime := time.Now()
	n, e := i.prov.ReadAt(buffer, offset)
	if e != nil {
		atomic.AddUint64(&i.metricReadErrors, 1)
	} else {
		atomic.AddUint64(&i.metricReadTime, uint64(time.Since(ctime).Nanoseconds()))
	}
	return n, e
}

func (i *Metrics) WriteAt(buffer []byte, offset int64) (int, error) {
	cc := atomic.AddInt64(&i.metricCurrentWriteOps, 1)
	defer atomic.AddInt64(&i.metricCurrentWriteOps, -1)
	atomic.AddInt64(&i.metricWriteOpsConcurrency, cc)
	atomic.AddInt64(&i.metricOpsConcurrency, cc+atomic.LoadInt64(&i.metricCurrentReadOps))

	i.metricWriteOpsLock.Lock()
	num := len(buffer)
	for _, v := range OpSizeBuckets {
		if num <= v {
			i.metricWriteOpsSize[v]++
			break
		}
	}
	i.metricWriteOpsLock.Unlock()

	atomic.AddUint64(&i.metricWriteOps, 1)
	atomic.AddUint64(&i.metricWriteBytes, uint64(len(buffer)))
	ctime := time.Now()
	n, e := i.prov.WriteAt(buffer, offset)
	if e != nil {
		atomic.AddUint64(&i.metricWriteErrors, 1)
	} else {
		atomic.AddUint64(&i.metricWriteTime, uint64(time.Since(ctime).Nanoseconds()))
	}
	return n, e
}

func (i *Metrics) Flush() error {
	atomic.AddUint64(&i.metricFlushOps, 1)
	ctime := time.Now()
	e := i.prov.Flush()
	atomic.AddUint64(&i.metricFlushTime, uint64(time.Since(ctime).Nanoseconds()))
	if e != nil {
		atomic.AddUint64(&i.metricFlushErrors, 1)
	}
	return e
}

func (i *Metrics) Size() uint64 {
	return i.prov.Size()
}

func (i *Metrics) Close() error {
	return i.prov.Close()
}

func (i *Metrics) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
