package modules

import (
	"os"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/stretchr/testify/assert"
)

const testSchema = `
device Test1 {
	system = "memory"
	size = "8k"
}

device Test2 {
	system = "file"
	size = "2m"
	location = "./testdata/testfile"
}

device TestNew {
	system = "file"
	size = "1m"
	location = "./testdata/testfile_new"
}

device Test3 {
	system = "file"
	size = "4k"
	blocksize = 1024
	location = "./testdata/testfile_3/"
}
`

func setup(t *testing.T) map[string]storage.StorageProvider {
	s := new(config.SiloSchema)
	err := s.Decode([]byte(testSchema))
	assert.NoError(t, err)
	devs, err := NewDevices(s.Device)
	assert.NoError(t, err)
	defer func() {
		os.Remove("./testdata/testfile_new")
	}()

	assert.Equal(t, 4, len(devs))
	return devs
}

func TestSourcesExisting(t *testing.T) {
	devs := setup(t)

	buffer := []byte("Hello world testing 1 2 3")
	//	_, err = devs["Test2"].WriteAt(buffer, 400)
	//	assert.NoError(t, err)

	buff := make([]byte, len(buffer))
	_, err := devs["Test2"].ReadAt(buff, 400)
	assert.NoError(t, err)

	assert.Equal(t, buffer, buff)

	devs["Test2"].Close()
}

func TestSourcesNew(t *testing.T) {
	devs := setup(t)

	buffer := []byte("Hello world testing 1 2 3")
	_, err := devs["TestNew"].WriteAt(buffer, 400)
	assert.NoError(t, err)

	buff := make([]byte, len(buffer))
	_, err = devs["TestNew"].ReadAt(buff, 400)
	assert.NoError(t, err)

	assert.Equal(t, buffer, buff)

	devs["TestNew"].Close()
}

func TestSourcesExistingDir(t *testing.T) {
	devs := setup(t)

	buffer := []byte("Hello world testing 1 2 3")
	_, err := devs["Test3"].WriteAt(buffer, 1020)
	assert.NoError(t, err)

	buff := make([]byte, len(buffer))
	_, err = devs["Test3"].ReadAt(buff, 1020)
	assert.NoError(t, err)

	assert.Equal(t, buffer, buff)

	devs["Test3"].Close()
}
