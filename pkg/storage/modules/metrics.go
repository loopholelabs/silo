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
	prov               storage.StorageProvider
	metric_read_ops    uint64
	metric_read_bytes  uint64
	metric_read_time   uint64
	metric_write_ops   uint64
	metric_write_bytes uint64
	metric_write_time  uint64
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
	fmt.Printf("%s: Reads=%d (%d bytes) avg latency %dns\n",
		prefix,
		read_ops,
		atomic.LoadUint64(&i.metric_read_bytes),
		read_avg_time,
	)

	write_ops := atomic.LoadUint64(&i.metric_write_ops)
	write_time := atomic.LoadUint64(&i.metric_write_time)
	write_avg_time := uint64(0)
	if write_ops > 0 {
		write_avg_time = write_time / write_ops
	}
	fmt.Printf("%s: Write=%d (%d bytes) avg latency %dns\n",
		prefix,
		write_ops,
		atomic.LoadUint64(&i.metric_write_bytes),
		write_avg_time,
	)
}

func (i *Metrics) ResetMetrics() {
	atomic.StoreUint64(&i.metric_read_ops, 0)
	atomic.StoreUint64(&i.metric_read_bytes, 0)
	atomic.StoreUint64(&i.metric_write_ops, 0)
	atomic.StoreUint64(&i.metric_write_bytes, 0)
}

func (i *Metrics) ReadAt(buffer []byte, offset int64) (int, error) {
	atomic.AddUint64(&i.metric_read_ops, 1)
	atomic.AddUint64(&i.metric_read_bytes, uint64(len(buffer)))
	ctime := time.Now()
	n, e := i.prov.ReadAt(buffer, offset)
	atomic.AddUint64(&i.metric_read_time, uint64(time.Since(ctime).Nanoseconds()))
	return n, e
}

func (i *Metrics) WriteAt(buffer []byte, offset int64) (int, error) {
	atomic.AddUint64(&i.metric_write_ops, 1)
	atomic.AddUint64(&i.metric_write_bytes, uint64(len(buffer)))
	ctime := time.Now()
	n, e := i.prov.WriteAt(buffer, offset)
	atomic.AddUint64(&i.metric_write_time, uint64(time.Since(ctime).Nanoseconds()))
	return n, e
}

func (i *Metrics) Flush() error {
	return i.prov.Flush()
}

func (i *Metrics) Size() uint64 {
	return i.prov.Size()
}
