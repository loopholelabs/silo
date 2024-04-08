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

	// Check we can't read data that isn't there yet
	_, err = source.ReadAt(buffer, 50)
	assert.Error(t, err)
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

func TestFileStorageSparsePartialWrite(t *testing.T) {

	source, err := NewFileStorageSparseCreate("test_data_sparse", 100, 10)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	data := make([]byte, 50)
	rand.Read(data)

	// Do complete write for first 5 blocks
	_, err = source.WriteAt(data, 0)
	assert.NoError(t, err)

	// Try doing partial write
	buffer := make([]byte, 30)
	_, err = source.WriteAt(buffer, 5)
	assert.NoError(t, err)

	rbuffer := make([]byte, 50)
	_, err = source.ReadAt(rbuffer, 0)
	assert.NoError(t, err)

	copy(data[5:], buffer)

	assert.Equal(t, data, rbuffer)

	// Try doing partial write on blocks not there
	_, err = source.WriteAt(buffer, 55)
	assert.Error(t, err)
}

func TestFileStorageSparseOverrun(t *testing.T) {
	source, err := NewFileStorageSparseCreate("test_data_sparse", 100, 10)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	data := make([]byte, 50)
	rand.Read(data)

	n, err := source.WriteAt(data, 60)
	assert.NoError(t, err)
	assert.Equal(t, 40, n)

	n, err = source.ReadAt(data, 60)
	assert.NoError(t, err)
	assert.Equal(t, 40, n)

}
