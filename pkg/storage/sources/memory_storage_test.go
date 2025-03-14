package sources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryStorage(t *testing.T) {

	source := NewMemoryStorage(1024 * 1024)

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

func TestMemoryStorageOverrun(t *testing.T) {
	size := 100
	offset := 90

	source := NewMemoryStorage(size)

	data := []byte("Hello world this is a test")

	// At offset 90, we can only fit 10 bytes
	n, err := source.WriteAt(data, int64(offset))
	assert.NoError(t, err)
	assert.Equal(t, size-offset, n)

	// Try reading it back...

	buffer := make([]byte, len(data))
	n, err = source.ReadAt(buffer, int64(offset))
	assert.NoError(t, err)
	assert.Equal(t, size-offset, n)

	assert.Equal(t, string(data[:10]), string(buffer[:10]))
}
