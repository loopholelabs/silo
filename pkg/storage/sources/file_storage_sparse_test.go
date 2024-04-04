package sources

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileStorageSparseCreate(t *testing.T) {

	source, err := NewFileStorageSparseCreate("test_data_sparse", 100, 10)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	data := make([]byte, 30)
	rand.Read(data)

	_, err = source.WriteAt(data, 0)
	assert.NoError(t, err)

	// Try reading it back...
	buffer := make([]byte, len(data))
	_, err = source.ReadAt(buffer, 0)
	assert.NoError(t, err)

	assert.Equal(t, data, buffer)
}

func TestFileStorageSparsePartialRead(t *testing.T) {

	source, err := NewFileStorageSparseCreate("test_data_sparse", 100, 10)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	data := make([]byte, 30)
	rand.Read(data)

	_, err = source.WriteAt(data, 0)
	assert.NoError(t, err)

	// Try reading it back...
	buffer := make([]byte, len(data)-6) // Take off 3 bytes either end
	_, err = source.ReadAt(buffer, 3)
	assert.NoError(t, err)

	assert.Equal(t, data[3:len(data)-3], buffer)
}

func TestFileStorageSparse(t *testing.T) {

	source, err := NewFileStorageSparseCreate("test_data_sparse", 100, 10)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	data := make([]byte, 30)
	rand.Read(data)

	_, err = source.WriteAt(data, 10)
	assert.NoError(t, err)

	// Try reading it back...

	buffer := make([]byte, len(data))
	_, err = source.ReadAt(buffer, 10)
	assert.NoError(t, err)

	assert.Equal(t, data, buffer)

	source2, err := NewFileStorageSparse("test_data_sparse", 100, 10)
	assert.NoError(t, err)

	buffer2 := make([]byte, len(data))
	_, err = source2.ReadAt(buffer2, 10)
	assert.NoError(t, err)

	assert.Equal(t, data, buffer2)
}
