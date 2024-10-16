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
	_, err = rand.Read(data)
	assert.NoError(t, err)

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
	_, err = rand.Read(data)
	assert.NoError(t, err)

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
	_, err = rand.Read(data)
	assert.NoError(t, err)

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
	_, err = rand.Read(data)
	assert.NoError(t, err)

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
	_, err = rand.Read(data)
	assert.NoError(t, err)

	n, err := source.WriteAt(data, 60)
	assert.NoError(t, err)
	assert.Equal(t, 40, n)

	n, err = source.ReadAt(data, 60)
	assert.NoError(t, err)
	assert.Equal(t, 40, n)

}

func TestFileStorageSparseNonMultiple(t *testing.T) {
	source, err := NewFileStorageSparseCreate("test_data_sparse", 102, 10)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	data := make([]byte, 50)
	_, err = rand.Read(data)
	assert.NoError(t, err)

	n, err := source.WriteAt(data, 60)
	assert.NoError(t, err)
	assert.Equal(t, 42, n)

	data2 := make([]byte, 50)
	n, err = source.ReadAt(data2, 60)
	assert.NoError(t, err)
	assert.Equal(t, 42, n)

	assert.Equal(t, data[:42], data2[:42])
}

func TestFileStorageSparseResume(t *testing.T) {

	source, err := NewFileStorageSparseCreate("test_data_sparse", 100, 10)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	data := make([]byte, 30)
	_, err = rand.Read(data)
	assert.NoError(t, err)

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

	// Write some new data
	data2 := make([]byte, 30)
	_, err = rand.Read(data2)
	assert.NoError(t, err)
	_, err = source2.WriteAt(data2, 50)
	assert.NoError(t, err)

	// Check it got written correctly...
	_, err = source2.ReadAt(buffer, 50)
	assert.NoError(t, err)

	assert.Equal(t, data2, buffer)

	_, err = source2.ReadAt(buffer, 10)
	assert.NoError(t, err)

	assert.Equal(t, data, buffer)
}

func TestFileStorageSparsePartialLastBlock(t *testing.T) {

	// Here we have a sparse file.
	// We have a block size of 100.
	// We have a total size of 950.

	// The issue comes when we do a write invloving the last block,
	// but that doesn't extend to the END of the storage size.
	// In this case we MUST read the block and merge the data in before writing back.
	//

	source, err := NewFileStorageSparseCreate("test_data_sparse", 950, 100)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	// Start by writing random data to the source.
	data := make([]byte, source.Size())
	_, err = rand.Read(data)
	assert.NoError(t, err)

	_, err = source.WriteAt(data, 0)
	assert.NoError(t, err)

	// Now do a partial write on a block boundary, which doesn't span to the end of the storage size.
	buffer := make([]byte, 30)
	_, err = rand.Read(buffer)
	assert.NoError(t, err)
	_, err = source.WriteAt(buffer, 900)
	assert.NoError(t, err)

	// Manually do the write in our buffer to compare with later.
	copy(data[900:], buffer)

	// Get all the data...
	rbuffer := make([]byte, source.Size())
	_, err = source.ReadAt(rbuffer, 0)
	assert.NoError(t, err)

	// Compare
	assert.Equal(t, data, rbuffer)
}
