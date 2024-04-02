package device

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/stretchr/testify/assert"
)

const testCowSchema = `
device TestCow {
	system = "sparsefile"
	size = "2m"
	blocksize = "64k"
	location = "./testdata/testfile_cow"
	source "cow" {
		system = "file"
		size = "2m"
		location = "./testdata/testfile_cow_src"
	}
}
`

func setupCow(t *testing.T) map[string]*Device {
	s := new(config.SiloSchema)
	err := s.Decode([]byte(testCowSchema))
	assert.NoError(t, err)
	devs, err := NewDevices(s.Device)
	assert.NoError(t, err)
	defer func() {
		os.Remove("./testdata/testfile_cow")
		os.Remove("./testdata/testfile_cow.offsets")
		os.Remove("./testdata/testfile_cow_src")
	}()

	assert.Equal(t, 1, len(devs))
	return devs
}

func TestSourceCow(t *testing.T) {
	// Create a ROSource file
	cowData := make([]byte, 2*1024*1024)
	rand.Read(cowData)

	err := os.WriteFile("./testdata/testfile_cow_src", cowData, 0666)
	if err != nil {
		panic(err)
	}

	devs := setupCow(t)

	buffer := []byte("Hello world testing 1 2 3")
	_, err = devs["TestCow"].Provider.WriteAt(buffer, 400)
	assert.NoError(t, err)

	// Now do some reading...
	buff := make([]byte, 2*1024*1024)
	_, err = devs["TestCow"].Provider.ReadAt(buff, 0)
	assert.NoError(t, err)

	// Do the write here as well
	copy(cowData[400:], buffer)

	// Make sure they're equal
	assert.Equal(t, cowData, buff)

	devs["TestCow"].Provider.Close()
}
