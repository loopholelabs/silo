package dirtytracker

import (
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestReadDirtyTracker(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	metrics := modules.NewMetrics(mem)
	trackerLocal, trackerRemote := NewDirtyTracker(metrics, 4096)

	b := trackerRemote.Sync()

	// There should be no dirty blocks
	assert.Equal(t, 0, b.Count(0, b.Length()))

	// Perform a read to start tracking dirty writes
	buffer := make([]byte, 1234567)
	_, err := trackerRemote.ReadAt(buffer, 10)
	assert.NoError(t, err)

	// Now do a few writes to make dirty blocks...
	locs := []int64{10, 10000, 40000}
	for _, l := range locs {
		w_buffer := make([]byte, 9000)
		_, err = trackerLocal.WriteAt(w_buffer, l)
		assert.NoError(t, err)
	}

	// Check the dirty blocks
	b = trackerRemote.Sync()
	assert.Equal(t, 8, b.Count(0, b.Length()))
	blocks := b.Collect(0, b.Length())
	expected_blocks := []uint{0, 1, 2, 3, 4, 9, 10, 11}
	assert.Equal(t, expected_blocks, blocks)

	b = trackerRemote.Sync()

	// There should be no dirty blocks
	assert.Equal(t, 0, b.Count(0, b.Length()))
}