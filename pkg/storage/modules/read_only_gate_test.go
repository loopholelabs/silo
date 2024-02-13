package modules

import (
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestReadOnlyGate(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	metrics := NewMetrics(mem)
	gate := NewReadOnlyGate(metrics)

	// First try a write
	buffer := make([]byte, 1234567)
	_, err := gate.WriteAt(buffer, 10)
	assert.NoError(t, err)

	// Now lock and wait...
	gate.Lock()

	// Reads should still get through
	_, err = gate.ReadAt(buffer, 10)
	assert.NoError(t, err)

	go func() {
		time.Sleep(50 * time.Millisecond)
		gate.Unlock()
	}()

	ctime := time.Now()
	_, err = gate.WriteAt(buffer, 10)
	assert.NoError(t, err)
	d := time.Since(ctime).Milliseconds()

	// Should have taken around 50ms
	assert.InDelta(t, 50, d, 10)
}
