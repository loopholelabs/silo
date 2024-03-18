package modules

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 * Simple metrics filter for reader/writer
 *
 */
type Metrics struct {
	prov                storage.StorageProvider
	metric_read_ops     uint64
	metric_read_bytes   uint64
	metric_read_time    uint64
	metric_read_errors  uint64
	metric_write_ops    uint64
	metric_write_bytes  uint64
	metric_write_time   uint64
	metric_write_errors uint64
	metric_flush_ops    uint64
	metric_flush_time   uint64
	metric_flush_errors uint64
}

type MetricsSnapshot struct {
	Read_ops     uint64
	Read_bytes   uint64
	Read_time    uint64
	Read_errors  uint64
	Write_ops    uint64
	Write_bytes  uint64
	Write_time   uint64
	Write_errors uint64
	Flush_ops    uint64
	Flush_time   uint64
	Flush_errors uint64
}

func NewMetrics(prov storage.StorageProvider) *Metrics {
	return &Metrics{
		prov: prov,
	}
}

func (i *Metrics) ShowStats(prefix string) {
	read_ops := atomic.LoadUint64(&i.metric_read_ops)
	read_time := atomic.LoadUint64(&i.metric_read_time)
	read_avg_time := uint64(0)
	if read_ops > 0 {
		read_avg_time = read_time / read_ops
	}
	fmt.Printf("%s: Reads=%d (%d bytes) avg latency %dns, %d errors, ",
		prefix,
		read_ops,
		atomic.LoadUint64(&i.metric_read_bytes),
		read_avg_time,
		atomic.LoadUint64(&i.metric_read_errors),
	)

	write_ops := atomic.LoadUint64(&i.metric_write_ops)
	write_time := atomic.LoadUint64(&i.metric_write_time)
	write_avg_time := uint64(0)
	if write_ops > 0 {
		write_avg_time = write_time / write_ops
	}
	fmt.Printf("Writes=%d (%d bytes) avg latency %dns, %d errors, ",
		write_ops,
		atomic.LoadUint64(&i.metric_write_bytes),
		write_avg_time,
		atomic.LoadUint64(&i.metric_write_errors),
	)

	flush_ops := atomic.LoadUint64(&i.metric_flush_ops)
	flush_time := atomic.LoadUint64(&i.metric_flush_time)
	flush_avg_time := uint64(0)
	if flush_ops > 0 {
		flush_avg_time = flush_time / flush_ops
	}
	fmt.Printf("Flushes=%d avg latency %dns, %d errors\n",
		flush_ops,
		flush_avg_time,
		atomic.LoadUint64(&i.metric_flush_errors),
	)
}

func (i *Metrics) Snapshot() *MetricsSnapshot {
	return &MetricsSnapshot{
		Read_ops:     atomic.LoadUint64(&i.metric_read_ops),
		Read_bytes:   atomic.LoadUint64(&i.metric_read_bytes),
		Read_time:    atomic.LoadUint64(&i.metric_read_time),
		Read_errors:  atomic.LoadUint64(&i.metric_read_errors),
		Write_ops:    atomic.LoadUint64(&i.metric_write_ops),
		Write_bytes:  atomic.LoadUint64(&i.metric_write_bytes),
		Write_time:   atomic.LoadUint64(&i.metric_write_time),
		Write_errors: atomic.LoadUint64(&i.metric_write_errors),
		Flush_ops:    atomic.LoadUint64(&i.metric_flush_ops),
		Flush_time:   atomic.LoadUint64(&i.metric_flush_time),
		Flush_errors: atomic.LoadUint64(&i.metric_flush_errors),
	}
}

func (i *Metrics) ResetMetrics() {
	atomic.StoreUint64(&i.metric_read_ops, 0)
	atomic.StoreUint64(&i.metric_read_bytes, 0)
	atomic.StoreUint64(&i.metric_read_time, 0)
	atomic.StoreUint64(&i.metric_read_errors, 0)
	atomic.StoreUint64(&i.metric_write_ops, 0)
	atomic.StoreUint64(&i.metric_write_bytes, 0)
	atomic.StoreUint64(&i.metric_write_time, 0)
	atomic.StoreUint64(&i.metric_write_errors, 0)
	atomic.StoreUint64(&i.metric_flush_ops, 0)
	atomic.StoreUint64(&i.metric_flush_time, 0)
	atomic.StoreUint64(&i.metric_flush_errors, 0)
}

func (i *Metrics) ReadAt(buffer []byte, offset int64) (int, error) {
	atomic.AddUint64(&i.metric_read_ops, 1)
	atomic.AddUint64(&i.metric_read_bytes, uint64(len(buffer)))
	ctime := time.Now()
	n, e := i.prov.ReadAt(buffer, offset)
	atomic.AddUint64(&i.metric_read_time, uint64(time.Since(ctime).Nanoseconds()))
	if e != nil {
		atomic.AddUint64(&i.metric_read_errors, 1)
	}
	return n, e
}

func (i *Metrics) WriteAt(buffer []byte, offset int64) (int, error) {
	atomic.AddUint64(&i.metric_write_ops, 1)
	atomic.AddUint64(&i.metric_write_bytes, uint64(len(buffer)))
	ctime := time.Now()
	n, e := i.prov.WriteAt(buffer, offset)
	atomic.AddUint64(&i.metric_write_time, uint64(time.Since(ctime).Nanoseconds()))
	if e != nil {
		atomic.AddUint64(&i.metric_write_errors, 1)
	}
	return n, e
}

func (i *Metrics) Flush() error {
	atomic.AddUint64(&i.metric_flush_ops, 1)
	ctime := time.Now()
	e := i.prov.Flush()
	atomic.AddUint64(&i.metric_flush_time, uint64(time.Since(ctime).Nanoseconds()))
	if e != nil {
		atomic.AddUint64(&i.metric_flush_errors, 1)
	}
	return e
}

func (i *Metrics) Size() uint64 {
	return i.prov.Size()
}

func (i *Metrics) Close() error {
	return i.prov.Close()
}
