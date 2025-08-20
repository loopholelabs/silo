package swarming

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestHashBlockManager(t *testing.T) {

	hbm := NewHashBlockManager()

	dataSource := sources.NewMemoryStorage(8 * 1024 * 1024)

	buffer := make([]byte, dataSource.Size())
	rand.Read(buffer)
	_, err := dataSource.WriteAt(buffer, 0)
	assert.NoError(t, err)

	// Index the data by block
	blockSize := 1024 * 1024
	hbm.IndexStorage(dataSource, blockSize)

	// Now try getting the data...
	data := make([]byte, blockSize)
	for offset := uint64(0); offset < dataSource.Size(); offset += uint64(blockSize) {
		n, err := dataSource.ReadAt(data, int64(offset))
		assert.NoError(t, err)

		hash := sha256.Sum256(data[:n])

		data2, err := hbm.Get(fmt.Sprintf("%x", hash))
		assert.NoError(t, err)

		assert.Equal(t, data, data2)
	}

	// Make sure we can't get some non-existant block
	hash := make([]byte, sha256.Size)
	rand.Read(hash)
	_, err = hbm.Get(fmt.Sprintf("%x", hash))
	assert.ErrorIs(t, err, ErrBlockNotFound)

}
