package sources

import (
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestBlockStorage(t *testing.T) {
	cr := func(s int) storage.StorageProvider {
		return NewMemoryStorage(s)
	}

	// Create a new block storage, backed by memory storage
	source := NewShardedStorage(1024*1024, 1024, cr)

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
