package sources_test

import (
	"crypto/rand"
	"fmt"
	"testing"

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
	rand.Read(buffer)
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
