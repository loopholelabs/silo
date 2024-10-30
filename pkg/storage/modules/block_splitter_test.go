package modules

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestBlockSplitterRead(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	//	source := NewArtificialLatency(mem, 10*time.Millisecond, 100*time.Nanosecond, 10*time.Millisecond, 100*time.Nanosecond)
	metrics := NewMetrics(mem)
	// log := NewLogger(metrics)
	split := NewBlockSplitter(metrics, 100) // Block size of 100 bytes

	// Fill it with stuff
	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	_, err = mem.WriteAt(data, 0)
	assert.NoError(t, err)

	readLen := 1234567

	// Read using a splitter (splits the read up into concurrent block reads)
	buffer := make([]byte, readLen)
	ctime1 := time.Now()
	_, err = split.ReadAt(buffer, 10)
	assert.NoError(t, err)
	readDurationSplit := time.Since(ctime1)

	assert.NoError(t, err)

	// Read in a single go from the source itself
	buffer2 := make([]byte, readLen)
	ctime2 := time.Now()
	_, err = mem.ReadAt(buffer2, 10)
	assert.NoError(t, err)
	readDuration := time.Since(ctime2)

	assert.Equal(t, buffer2, buffer)

	fmt.Printf("Read took %dms. Read split took %dms.\n", readDuration.Milliseconds(), readDurationSplit.Milliseconds())
	//
	// metrics.ShowStats("Source")
}

func TestBlockSplitterWrite(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	// metrics := NewMetrics(mem)
	split := NewBlockSplitter(mem, 100) // Block size of 100 bytes

	writeLen := 1234567

	// Write some stuff to the mem...
	b := make([]byte, size)
	_, err := rand.Read(b)
	assert.NoError(t, err)
	_, err = mem.WriteAt(b, 0)
	assert.NoError(t, err)

	offset := int64(10)
	// Write using a splitter (splits the read up into concurrent block writes)
	buffer := make([]byte, writeLen)
	_, err = rand.Read(buffer)
	assert.NoError(t, err)
	_, err = split.WriteAt(buffer, offset)
	assert.NoError(t, err)

	// Check that the write was performed properly and didn't mess up anything

	copy(b[offset:], buffer) // Perform the write in the b buffer.

	check := make([]byte, size)
	_, err = mem.ReadAt(check, 0)
	assert.NoError(t, err)

	assert.Equal(t, b, check)

	// metrics.ShowStats("source")
}
