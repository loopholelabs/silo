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
	size = "4096"
	blocksize = "1024"
	location = "./testdata/testfile_cow"
	source "./testdata/cow_state" {
		system = "file"
		size = "4096"
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
	t.Cleanup(func() {
		os.Remove("./testdata/testfile_cow")
		os.Remove("./testdata/testfile_cow_src")
		os.Remove("./testdata/cow_state")
	})

	assert.Equal(t, 1, len(devs))
	return devs
}

func TestSourceCow(t *testing.T) {
	// Create a ROSource file
	cowData := make([]byte, 4*1024)
	_, err := rand.Read(cowData)
	assert.NoError(t, err)

	err = os.WriteFile("./testdata/testfile_cow_src", cowData, 0666)
	if err != nil {
		panic(err)
	}

	devs := setupCow(t)

	buffer := []byte("Hello world testing 1 2 3")
	n, err := devs["TestCow"].Provider.WriteAt(buffer, 400)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	// Now do some reading...
	buff := make([]byte, len(cowData))
	n, err = devs["TestCow"].Provider.ReadAt(buff, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buff), n)

	// Do the write here as well
	copy(cowData[400:], buffer)

	// Make sure they're equal
	assert.Equal(t, cowData, buff)

	devs["TestCow"].Provider.Close()

}
