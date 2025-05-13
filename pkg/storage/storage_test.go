package storage_test

import (
	"crypto/rand"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestStorageDifference(t *testing.T) {
	sp1 := sources.NewMemoryStorage(1024 * 1024)
	sp2 := sources.NewMemoryStorage(1024 * 1024)

	d := make([]byte, sp1.Size())
	_, err := rand.Read(d)
	assert.NoError(t, err)
	n, err := sp1.WriteAt(d, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(d), n)
	n, err = sp2.WriteAt(d, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(d), n)

	// Now modify sp2 a bit
	for _, offset := range []int64{7, 8, 900, 3000} {
		modd := make([]byte, 6)
		_, err = rand.Read(modd)
		assert.NoError(t, err)
		// Make sure it's different... (Otherwise this is a flaky test)
		for i := range modd {
			if d[offset+int64(i)] == modd[i] {
				modd[i]++
			}
		}

		n, err = sp2.WriteAt(modd, offset)
		assert.NoError(t, err)
		assert.Equal(t, len(modd), n)
	}

	diffBlocks, diffBytes, err := storage.Difference(sp1, sp2, 1024)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), diffBlocks)
	assert.Equal(t, uint64(19), diffBytes)
}

func TestEquals(t *testing.T) {
	sp1 := sources.NewMemoryStorage(1024 * 1024)
	sp2 := sources.NewMemoryStorage(1024 * 1024)

	d := make([]byte, sp1.Size())
	_, err := rand.Read(d)
	assert.NoError(t, err)
	n, err := sp1.WriteAt(d, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(d), n)
	n, err = sp2.WriteAt(d, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(d), n)

	eq, err := storage.Equals(sp1, sp2, 1024)
	assert.NoError(t, err)
	assert.True(t, eq)

	// Now modify sp2 a bit
	for _, offset := range []int64{7, 8, 900, 3000} {
		d = make([]byte, 6)
		_, err = rand.Read(d)
		assert.NoError(t, err)
		n, err = sp2.WriteAt(d, offset)
		assert.NoError(t, err)
		assert.Equal(t, len(d), n)
	}

	eq, err = storage.Equals(sp1, sp2, 1024)
	assert.NoError(t, err)
	assert.False(t, eq)

}
