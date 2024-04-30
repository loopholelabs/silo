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
	PORT_9000 := testutils.SetupMinio(t.Cleanup)

	size := 64 * 1024
	blockSize := 1024
	s3store, err := sources.NewS3StorageCreate(fmt.Sprintf("localhost:%s", PORT_9000), "silosilo", "silosilo", "silosilo", "file", uint64(size), blockSize)
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
	PORT_9000 := testutils.SetupMinio(t.Cleanup)

	size := 64 * 1024
	blockSize := 1024
	s3store, err := sources.NewS3StorageCreate(fmt.Sprintf("localhost:%s", PORT_9000), "silosilo", "silosilo", "silosilo", "file", uint64(size), blockSize)
	assert.NoError(t, err)

	buffer := make([]byte, 32*1024)
	_, err = rand.Read(buffer)
	assert.NoError(t, err)

	// Cancel writes just after the WriteAt()
	time.AfterFunc(10*time.Millisecond, func() {
		s3store.CancelWrites(0, int64(size)) // Cancel ALL writes!
	})

	_, err = s3store.WriteAt(buffer, 80)
	assert.ErrorIs(t, err, context.Canceled)

	err = s3store.Close()
	assert.NoError(t, err)
}
