package modules

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestBlockSplitter(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	source := NewArtificialLatency(mem, 10*time.Millisecond, 100*time.Nanosecond, 10*time.Millisecond, 100*time.Nanosecond)
	metrics := NewMetrics(source)
	log := NewLogger(metrics)
	split := NewBlockSplitter(log, 100) // Block size of 100 bytes

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
	_, err = source.ReadAt(buffer2, 10)
	read_duration := time.Since(ctime_2)

	assert.Equal(t, buffer2, buffer)

	fmt.Printf("Read took %dms. Read split took %dms.\n", read_duration.Milliseconds(), read_duration_split.Milliseconds())
	metrics.ShowStats("Source")
}
