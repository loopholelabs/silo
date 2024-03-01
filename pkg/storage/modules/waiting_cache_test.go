package modules

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestWaitingCache(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	metrics := NewMetrics(mem)
	waitingLocal, waitingRemote := NewWaitingCache(metrics, 4096)

	data := make([]byte, 12000)
	rand.Read(data)

	// We'll write something in 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := waitingRemote.WriteAt(data, 0)
		assert.NoError(t, err)
	}()

	// The waiting cache will wait for data to be available.
	offset := int64(20)
	buffer := make([]byte, 8000)
	ctime := time.Now()
	_, err := waitingLocal.ReadAt(buffer, offset)
	assert.NoError(t, err)
	wait_time := time.Since(ctime).Milliseconds()

	// We'd expect this read to take around 50ms (It's waiting for the data)
	assert.InDelta(t, wait_time, 50, 10)

	assert.Equal(t, data[offset:int(offset)+len(buffer)], buffer)

	// Read again
	ctime2 := time.Now()
	_, err = waitingLocal.ReadAt(buffer, offset)
	assert.NoError(t, err)
	wait_time2 := time.Since(ctime2).Milliseconds()

	// We'd expect this read to be instant (The data exists now)
	assert.InDelta(t, wait_time2, 0, 10)

}

func TestWaitingCachePartial(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 6000
	mem := sources.NewMemoryStorage(size)
	metrics := NewMetrics(mem)
	waitingLocal, waitingRemote := NewWaitingCache(metrics, 4096)

	data := make([]byte, 6000)
	rand.Read(data)

	// We'll write something in 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := waitingRemote.WriteAt(data, 0)
		assert.NoError(t, err)
	}()

	// The waiting cache will wait for data to be available.
	offset := int64(0)
	buffer := make([]byte, 6000)
	ctime := time.Now()
	_, err := waitingLocal.ReadAt(buffer, offset)
	assert.NoError(t, err)
	wait_time := time.Since(ctime).Milliseconds()

	// We'd expect this read to take around 50ms (It's waiting for the data)
	assert.InDelta(t, wait_time, 50, 10)

	assert.Equal(t, data[offset:int(offset)+len(buffer)], buffer)

	// Read again
	ctime2 := time.Now()
	_, err = waitingLocal.ReadAt(buffer, offset)
	assert.NoError(t, err)
	wait_time2 := time.Since(ctime2).Milliseconds()

	// We'd expect this read to be instant (The data exists now)
	assert.InDelta(t, wait_time2, 0, 10)

}

func TestWaitingCacheLocalWrites(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024 * 32
	mem := sources.NewMemoryStorage(size)
	metrics := NewMetrics(mem)
	waitingLocal, waitingRemote := NewWaitingCache(metrics, 4096)

	// Try complete blocks
	data := make([]byte, 8192)
	rand.Read(data)

	// We'll write something in 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := waitingLocal.WriteAt(data, 0)
		assert.NoError(t, err)
	}()

	// The waiting cache will wait for data to be available.
	offset := int64(4096)
	buffer := make([]byte, 4096)
	ctime := time.Now()
	_, err := waitingLocal.ReadAt(buffer, offset)
	assert.NoError(t, err)
	wait_time := time.Since(ctime).Milliseconds()

	// We'd expect this read to take around 50ms (It's waiting for the data)
	assert.InDelta(t, wait_time, 50, 10)

	assert.Equal(t, data[offset:int(offset)+len(buffer)], buffer)

	// Write from remote

	dataRemote := make([]byte, 8192)
	rand.Read(dataRemote)
	_, err = waitingRemote.WriteAt(dataRemote, 0)
	assert.NoError(t, err)

	// Read again
	ctime2 := time.Now()
	_, err = waitingLocal.ReadAt(buffer, offset)
	assert.NoError(t, err)
	wait_time2 := time.Since(ctime2).Milliseconds()

	// We'd expect this read to be instant (The data exists now)
	assert.InDelta(t, wait_time2, 0, 10)

	// It should be the local data, not the remote
	assert.Equal(t, data[offset:int(offset)+len(buffer)], buffer)

}
