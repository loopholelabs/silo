package modules

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestBlockCache(t *testing.T) {
	size := 1024 * 1024
	source1 := sources.NewMemoryStorage(size)
	source2 := sources.NewMemoryStorage(size)
	s1 := NewMetrics(source1)
	s2 := NewMetrics(source2)

	bc := NewBlockCache(s1, 1024, 800)

	// Now do writes to both and make sure they're equal. Also Make sure the number of writes is less.

	concurrency := make(chan bool, 100)

	// Number of writes to perform
	totalWrites := 1000000

	var wg sync.WaitGroup
	for i := 0; i < totalWrites; i++ {
		concurrency <- true
		wg.Add(1)
		go func() {

			// Do random writes
			offset := rand.Intn(size)
			length := rand.Intn(2048)
			if offset+length >= size {
				length = size - offset
			}
			buffer := make([]byte, length)
			crand.Read(buffer)
			n1, err := bc.WriteAt(buffer, int64(offset))
			assert.NoError(t, err)
			n2, err := s2.WriteAt(buffer, int64(offset))
			assert.NoError(t, err)
			assert.Equal(t, n1, n2)

			wg.Done()
			<-concurrency
		}()
	}

	wg.Wait()
	err := bc.Flush()
	assert.NoError(t, err)

	// NB These won't be equal always since the order of writes may be different if overlapping.
	// eq, err := storage.Equals(source1, source2, 1024)
	// assert.NoError(t, err)
	// assert.True(t, eq)

	met1 := s1.GetMetrics()
	met2 := s2.GetMetrics()

	fmt.Printf("CACHED %d writes %d bytes %d time\n", met1.WriteOps, met1.WriteBytes, met1.WriteTime)
	fmt.Printf("SOURCE %d writes %d bytes %d time\n", met2.WriteOps, met2.WriteBytes, met2.WriteTime)

	assert.Less(t, met1.WriteOps, met2.WriteOps)
}
