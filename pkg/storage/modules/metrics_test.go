package modules

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	met := NewMetrics(NewNothing())

	// Do some things...

	buffer := make([]byte, 4096)
	met.ReadAt(buffer, 0)

	met.WriteAt(buffer, 0)

	met.Flush()

	assert.Equal(t, uint64(1), met.metric_read_ops)
	assert.Equal(t, uint64(1), met.metric_write_ops)
	assert.Equal(t, uint64(1), met.metric_flush_ops)

	met.ResetMetrics()

	assert.Equal(t, uint64(0), met.metric_read_ops)
	assert.Equal(t, uint64(0), met.metric_write_ops)
	assert.Equal(t, uint64(0), met.metric_flush_ops)

	/*
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
	*/
}
