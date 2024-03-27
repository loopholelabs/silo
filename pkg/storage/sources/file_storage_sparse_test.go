package sources

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileStorageSparse(t *testing.T) {

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
