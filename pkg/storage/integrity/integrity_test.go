package integrity

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestIntegrity(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096

	mem := sources.NewMemoryStorage(size)

	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	n, err := mem.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	in := NewChecker(int64(size), blockSize)

	err = in.Hash(mem)
	assert.NoError(t, err)

	// Now do an integrity check...

	equals, err := in.Check(mem)
	assert.NoError(t, err)
	assert.Equal(t, true, equals)
}

func TestIntegrityChangedData(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096

	mem := sources.NewMemoryStorage(size)

	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	n, err := mem.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	in := NewChecker(int64(size), blockSize)

	err = in.Hash(mem)
	assert.NoError(t, err)

	// Change the data a bit...
	buffer := []byte("Hello world")
	n, err = mem.WriteAt(buffer, 100)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)
	// Now do an integrity check...

	equals, err := in.Check(mem)
	assert.NoError(t, err)
	assert.Equal(t, false, equals)
}

func TestIntegrityHashChanged(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096

	mem := sources.NewMemoryStorage(size)

	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	n, err := mem.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	in := NewChecker(int64(size), blockSize)

	err = in.Hash(mem)
	assert.NoError(t, err)

	in.SetHash(0, [sha256.Size]byte{}) // Clear out one of the hashes

	// Now do an integrity check...

	equals, err := in.Check(mem)
	assert.NoError(t, err)
	assert.Equal(t, false, equals)
}
