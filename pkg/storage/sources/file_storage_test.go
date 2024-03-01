package sources

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileStorage(t *testing.T) {

	source, err := NewFileStorageCreate("test_data", 1024*1024)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data")
	})

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

func TestFileStorageOverrun(t *testing.T) {
	size := 100
	offset := 90

	source, err := NewFileStorageCreate("test_data", int64(size))
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data")
	})

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
