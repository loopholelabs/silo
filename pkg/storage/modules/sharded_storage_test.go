package modules

import (
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestShardedStorage(t *testing.T) {
	cr := func(i int, s int) (storage.StorageProvider, error) {
		return sources.NewMemoryStorage(s), nil
	}

	// Create a new block storage, backed by memory storage
	source, err := NewShardedStorage(1024*1024, 1024, cr)
	assert.NoError(t, err)

	data := []byte("Hello world")

	n, err := source.WriteAt(data, 17)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Try reading it back...

	buffer := make([]byte, len(data))
	n, err = source.ReadAt(buffer, 17)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	assert.Equal(t, string(data), string(buffer))
}

func TestShardedStoragePartial(t *testing.T) {
	cr := func(i int, s int) (storage.StorageProvider, error) {
		return sources.NewMemoryStorage(s), nil
	}

	// Create a new block storage, backed by memory storage
	source, err := NewShardedStorage(6000, 4096, cr)
	assert.NoError(t, err)

	data := make([]byte, 6000)

	n, err := source.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Try reading it back...

	buffer := make([]byte, len(data))
	n, err = source.ReadAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

}
