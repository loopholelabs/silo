package modules

import (
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestWriteCombinatorBasic(t *testing.T) {
	storage := sources.NewMemoryStorage(64 * 1024)

	blockSize := 4 * 1024

	combinator := NewWriteCombinator(storage, blockSize)

	// Try doing a couple of writes and make sure they're dealt with correctly...
	source1 := combinator.AddSource(1)
	source2 := combinator.AddSource(2)

	// Make a couple of buffers with 1s and 2s in them so we can tell the data apart...
	buffer1 := make([]byte, blockSize)
	buffer2 := make([]byte, blockSize)
	for i := 0; i < blockSize; i++ {
		buffer1[i] = 1
		buffer2[i] = 2
	}

	// Do a couple of writes with different priority
	source1.WriteAt(buffer1, 0)
	source2.WriteAt(buffer2, 0)

	// Swap the order of the writes this time...
	source2.WriteAt(buffer2, int64(blockSize))
	source1.WriteAt(buffer1, int64(blockSize))

	// Now check... source2 should have won in both cases as it has higher priority...
	checkBuffer := make([]byte, blockSize*2)
	storage.ReadAt(checkBuffer, 0)
	for i := 0; i < len(checkBuffer); i++ {
		assert.Equal(t, uint8(2), checkBuffer[i])
	}

	met := combinator.GetMetrics()
	assert.Equal(t, map[int]uint64{1: 1, 2: 2}, met.WritesAllowed)
	assert.Equal(t, map[int]uint64{1: 1, 2: 0}, met.WritesBlocked)
}
