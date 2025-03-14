package device

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

const testBinlogOut = "test_binlog.out"
const testFileLoc = "test_device"

func TestDeviceBinlogReplay(t *testing.T) {
	t.Cleanup(func() {
		err := os.Remove(testFileLoc)
		assert.NoError(t, err)
		err = os.Remove(testBinlogOut)
		assert.NoError(t, err)
	})

	source := sources.NewMemoryStorage(1024 * 1024)

	dest := sources.NewMemoryStorage(1024 * 1024)
	destBinlog, err := modules.NewBinLog(dest, testBinlogOut)
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

	err = modules.CreateBinlogFromDevice(source, destBinlog, 1024)
	assert.NoError(t, err)

	// Make sure the devices are equal
	eq, err := storage.Equals(source, destBinlog, 1024)
	assert.NoError(t, err)
	assert.True(t, eq)

	destBinlog.Close()

	// Now try a replay with device

	d, _, err := NewDevice(&config.DeviceSchema{
		Name:       "test",
		Size:       fmt.Sprintf("%d", source.Size()),
		System:     "file",
		BlockSize:  "1024",
		Expose:     false,
		Location:   testFileLoc,
		LoadBinLog: testBinlogOut,
	})
	assert.NoError(t, err)

	// Now check these two devices are equal
	eq, err = storage.Equals(source, d, 1024)
	assert.NoError(t, err)
	assert.True(t, eq)

}
