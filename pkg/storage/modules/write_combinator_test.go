package modules

import (
	"math/rand"
	"sync"
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

func TestWriteCombinatorConcurrent(t *testing.T) {
	blockSize := 1024
	numBlocks := 64 * 1024

	storage := sources.NewMemoryStorage(blockSize * numBlocks)

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

	var wg sync.WaitGroup

	// Randomly write to the blocks in both goroutines.
	// Hopefully there will be some collisions.
	wg.Add(1)
	go func() {
		order1 := rand.Perm(numBlocks)
		for _, b := range order1 {
			_, err := source1.WriteAt(buffer1, int64(b*blockSize))
			assert.NoError(t, err)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		order2 := rand.Perm(numBlocks)
		for _, b := range order2 {
			_, err := source2.WriteAt(buffer2, int64(b*blockSize))
			assert.NoError(t, err)
		}
		wg.Done()
	}()

	// Wait for them...
	wg.Wait()

	// Now check... source2 should have won in all cases as it has higher priority...
	checkBuffer := make([]byte, storage.Size())
	storage.ReadAt(checkBuffer, 0)
	for i := 0; i < len(checkBuffer); i++ {
		assert.Equal(t, uint8(2), checkBuffer[i])
	}

	met := combinator.GetMetrics()

	assert.Equal(t, uint64(numBlocks), met.WritesAllowed[2]) // All of the writes from P2P should be allowed
	assert.Equal(t, uint64(0), met.WritesBlocked[2])         // None should be blocked
}
