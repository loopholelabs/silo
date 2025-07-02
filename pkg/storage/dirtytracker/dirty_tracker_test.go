package dirtytracker

import (
	"slices"
	"testing"
	"time"

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
		wBuffer := make([]byte, 9000)
		_, err = trackerLocal.WriteAt(wBuffer, l)
		assert.NoError(t, err)
	}

	// Check the dirty blocks
	b = trackerRemote.Sync()
	assert.Equal(t, 8, b.Count(0, b.Length()))
	blocks := b.Collect(0, b.Length())
	expectedBlocks := []uint{0, 1, 2, 3, 4, 9, 10, 11}
	assert.Equal(t, expectedBlocks, blocks)

	b = trackerRemote.Sync()

	// There should be no dirty blocks
	assert.Equal(t, 0, b.Count(0, b.Length()))
}

func setupDirty(t *testing.T) *Remote {
	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	trackerLocal, trackerRemote := NewDirtyTracker(mem, 4096)

	b := trackerRemote.Sync()

	// There should be no dirty blocks
	assert.Equal(t, 0, b.Count(0, b.Length()))

	// Perform a read to start tracking dirty writes all over the place
	buffer := make([]byte, size)
	_, err := trackerRemote.ReadAt(buffer, 0)
	assert.NoError(t, err)

	// Now do a few writes to make dirty blocks...
	locs := []int64{10, 30, 10000, 40000}
	for _, l := range locs {
		wBuffer := make([]byte, 9000)
		_, err = trackerLocal.WriteAt(wBuffer, l)
		assert.NoError(t, err)
	}

	return trackerRemote
}

func TestReadDirtyTrackerLimits(t *testing.T) {
	trackerRemote := setupDirty(t)

	// Check the dirty blocks
	blocks := trackerRemote.GetDirtyBlocks(0, 100, 1, 0)
	slices.Sort(blocks)
	expectedBlocks := []uint{0, 1, 2, 4, 5}
	assert.Equal(t, expectedBlocks, blocks)

	blocks = trackerRemote.GetDirtyBlocks(0, 100, 1, 0)

	// There should be no dirty blocks
	assert.Equal(t, 0, len(blocks))
}

func TestReadDirtyTrackerMaxAge(t *testing.T) {
	trackerRemote := setupDirty(t)

	// Check the dirty blocks
	blocks := trackerRemote.GetDirtyBlocks(100*time.Millisecond, 100, 1, 1000)
	assert.Equal(t, 0, len(blocks))

	time.Sleep(100 * time.Millisecond)

	// Things should expire now...
	blocks = trackerRemote.GetDirtyBlocks(100*time.Millisecond, 100, 1, 1000)
	slices.Sort(blocks)
	expectedBlocks := []uint{0, 1, 2, 4, 5}
	assert.Equal(t, expectedBlocks, blocks)
}

func TestReadDirtyTrackerMinChange(t *testing.T) {
	trackerRemote := setupDirty(t)

	// Check the dirty blocks
	blocks := trackerRemote.GetDirtyBlocks(time.Minute, 100, 1, 0)
	slices.Sort(blocks)
	expectedBlocks := []uint{0, 1, 2, 4, 5}
	assert.Equal(t, expectedBlocks, blocks)

	trackerRemote = setupDirty(t)
	blocks = trackerRemote.GetDirtyBlocks(time.Minute, 100, 1, 2)
	slices.Sort(blocks)
	expectedBlocks = []uint{0, 1, 5}
	assert.Equal(t, expectedBlocks, blocks)

	trackerRemote = setupDirty(t)
	blocks = trackerRemote.GetDirtyBlocks(time.Minute, 100, 2, 2)
	slices.Sort(blocks)
	expectedBlocks = []uint{0, 2}
	assert.Equal(t, expectedBlocks, blocks)
}
