package modules

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	met := NewMetrics(NewNothing(0))

	// Do some things...

	buffer := make([]byte, 4096)
	_, _ = met.ReadAt(buffer, 0)

	_, _ = met.WriteAt(buffer, 0)

	met.Flush()

	assert.Equal(t, uint64(1), met.metricReadOps)
	assert.Equal(t, uint64(1), met.metricWriteOps)
	assert.Equal(t, uint64(1), met.metricFlushOps)

	met.ResetMetrics()

	assert.Equal(t, uint64(0), met.metricReadOps)
	assert.Equal(t, uint64(0), met.metricWriteOps)
	assert.Equal(t, uint64(0), met.metricFlushOps)

}
