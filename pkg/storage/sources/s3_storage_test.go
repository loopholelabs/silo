package sources_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/testutils"

	"github.com/stretchr/testify/assert"
)

func TestS3Storage(t *testing.T) {
	MinioPort := testutils.SetupMinio(t.Cleanup)

	size := 64 * 1024
	blockSize := 1024
	s3store, err := sources.NewS3StorageCreate(false, fmt.Sprintf("localhost:%s", MinioPort), "silosilo", "silosilo", "silosilo", "file", uint64(size), blockSize)
	assert.NoError(t, err)

	buffer := make([]byte, 32*1024)
	_, err = rand.Read(buffer)
	assert.NoError(t, err)
	_, err = s3store.WriteAt(buffer, 80)
	assert.NoError(t, err)

	// Now read it all back...
	buffer2 := make([]byte, size)
	n, err := s3store.ReadAt(buffer2, 0)
	assert.Equal(t, size, n)
	assert.NoError(t, err)

	assert.Equal(t, buffer, buffer2[80:80+len(buffer)])

	err = s3store.Close()
	assert.NoError(t, err)
}

func TestS3StorageCancelWrites(t *testing.T) {
	MinioPort := testutils.SetupMinio(t.Cleanup)

	size := 64 * 1024
	blockSize := 1024
	s3store, err := sources.NewS3StorageCreate(false, fmt.Sprintf("localhost:%s", MinioPort), "silosilo", "silosilo", "silosilo", "file", uint64(size), blockSize)
	assert.NoError(t, err)

	buffer := make([]byte, 32*1024)
	_, err = rand.Read(buffer)
	assert.NoError(t, err)

	// Cancel writes just after the WriteAt()
	for _, delay := range []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond} {
		time.AfterFunc(delay, func() {
			s3store.CancelWrites(0, int64(size)) // Cancel ALL writes!
		})
	}

	// Do some writes...
	cancelled := 0
	for i := 0; i < 10; i++ {
		_, err = s3store.WriteAt(buffer, 80)
		// Either it succeeded before the cancel, or it will return canceled.
		if err != nil {
			assert.ErrorIs(t, err, context.Canceled)
			cancelled++
		}
	}

	assert.Greater(t, cancelled, 0)

	err = s3store.Close()
	assert.NoError(t, err)
}
