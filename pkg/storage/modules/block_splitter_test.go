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
	//log := NewLogger(metrics)
	split := NewBlockSplitter(metrics, 100) // Block size of 100 bytes

	// Fill it with stuff
	data := make([]byte, size)
	rand.Read(data)
	_, err := mem.WriteAt(data, 0)
	assert.NoError(t, err)

	read_len := 1234567

	// Read using a splitter (splits the read up into concurrent block reads)
	buffer := make([]byte, read_len)
	ctime_1 := time.Now()
	_, err = split.ReadAt(buffer, 10)
	read_duration_split := time.Since(ctime_1)

	assert.NoError(t, err)

	// Read in a single go from the source itself
	buffer2 := make([]byte, read_len)
	ctime_2 := time.Now()
	_, err = mem.ReadAt(buffer2, 10)
	read_duration := time.Since(ctime_2)

	assert.Equal(t, buffer2, buffer)

	fmt.Printf("Read took %dms. Read split took %dms.\n", read_duration.Milliseconds(), read_duration_split.Milliseconds())
	metrics.ShowStats("Source")
}

func TestBlockSplitterWrite(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	// metrics := NewMetrics(mem)
	split := NewBlockSplitter(mem, 100) // Block size of 100 bytes

	write_len := 1234567

	// Write some stuff to the mem...
	b := make([]byte, size)
	rand.Read(b)
	_, err := mem.WriteAt(b, 0)
	assert.NoError(t, err)

	offset := int64(10)
	// Write using a splitter (splits the read up into concurrent block writes)
	buffer := make([]byte, write_len)
	rand.Read(buffer)
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
