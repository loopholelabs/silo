package modules

import (
	"crypto/rand"
	"io"
	"os"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

const testBinlogOut = "test_binlog.out"

func TestBinlog(t *testing.T) {
	t.Cleanup(func() {
		err := os.Remove(testBinlogOut)
		assert.NoError(t, err)
	})

	source := sources.NewMemoryStorage(1024 * 1024)

	dest := sources.NewMemoryStorage(1024 * 1024)
	destBinlog, err := NewBinLog(dest, testBinlogOut)
	assert.NoError(t, err)

	// Do a few little writes

	data := make([]byte, 500)
	rand.Read(data)
	_, err = source.WriteAt(data, 100)
	assert.NoError(t, err)
	_, err = source.WriteAt(data, 900)
	assert.NoError(t, err)
	_, err = source.WriteAt(data, 20000)
	assert.NoError(t, err)

	// Create the binlog

	err = CreateBinlogFromDevice(source, destBinlog, 1024)
	assert.NoError(t, err)

	// Make sure the devices are equal
	eq, err := storage.Equals(source, destBinlog, 1024)
	assert.NoError(t, err)
	assert.True(t, eq)

	destBinlog.Close()

	// Now try a replay
	nextDevice := sources.NewMemoryStorage(1024 * 1024)
	binReplay, err := NewBinLogReplay(testBinlogOut, nextDevice)
	assert.NoError(t, err)

	for {
		provErr, err := binReplay.Next(0, true)
		assert.NoError(t, provErr)
		if err != nil {
			if err == io.EOF {
				break
			}
			assert.NoError(t, err) // Shouldn't be any other error
		}
	}

	// Now check these two devices are equal
	eq, err = storage.Equals(source, nextDevice, 1024)
	assert.NoError(t, err)
	assert.True(t, eq)

}
