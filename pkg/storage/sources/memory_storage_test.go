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
