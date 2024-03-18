package volatilitymonitor

import (
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestVolatilityMonitorTotal(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	metrics := modules.NewMetrics(mem)
	vol := NewVolatilityMonitor(metrics, 4096, 1*time.Second)

	offsets := []int64{10, 7000, 7004, 190000}
	buffer := make([]byte, 8192)

	for _, v := range offsets {
		_, err := vol.WriteAt(buffer, v)
		assert.NoError(t, err)
	}

	volatility := vol.GetTotalVolatility()

	assert.Equal(t, 12, volatility)
}

func TestVolatilityMonitor(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	metrics := modules.NewMetrics(mem)
	vol := NewVolatilityMonitor(metrics, 4096, 1*time.Second)

	offsets := []int64{10, 7000, 7004, 190000}
	buffer := make([]byte, 8192)

	// Add all blocks for monitoring
	for n := 0; n < (size / 4096); n++ {
		vol.Add(n)
	}

	// Perform some writes
	for _, v := range offsets {
		_, err := vol.WriteAt(buffer, v)
		assert.NoError(t, err)
	}

	expected := map[int]int{
		0:  1,
		1:  3,
		2:  3,
		3:  2,
		46: 1,
		47: 1,
		48: 1,
	}

	for block, val := range expected {
		v := vol.GetVolatility(block)
		assert.Equal(t, val, v)
	}
}

func TestVolatilityMonitorExpiry(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	metrics := modules.NewMetrics(mem)
	vol := NewVolatilityMonitor(metrics, 4096, 50*time.Millisecond)

	offsets := []int64{10, 7000, 7004, 190000}
	buffer := make([]byte, 8192)

	// Add all blocks for monitoring
	for n := 0; n < (size / 4096); n++ {
		vol.Add(n)
	}

	// Perform some writes
	for _, v := range offsets {
		_, err := vol.WriteAt(buffer, v)
		assert.NoError(t, err)
	}

	time.Sleep(55 * time.Millisecond)

	assert.Equal(t, 0, vol.GetTotalVolatility())

	for n := 0; n < (size / 4096); n++ {
		v := vol.GetVolatility(n)
		assert.Equal(t, 0, v)
	}
}
