package device

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/stretchr/testify/assert"
)

// Single COW config
const testCow1Schema = `
device TestCow {
	system = "file"
	size = "4096"
	blocksize = "1024"
	location = "./testdata/testfile_cow1"
	source "./testdata/cow1_state" {
		system = "file"
		size = "4096"
		blocksize = "1024"
		location = "./testdata/testfile_base"
	}
}
`

// Adding a second COW on top
const testCow2Schema = `
device TestCow {
	system = "file"
	size = "4096"
	blocksize = "1024"
	location = "./testdata/testfile_cow2"
	source "./testdata/cow2_state" {
		system = "file"
		size = "4096"
		blocksize = "1024"
		location = "./testdata/testfile_cow1"
		source "./testdata/cow1_state" {
			system = "file"
			size = "4096"
			blocksize = "1024"
			location = "./testdata/testfile_base"
		}
	}
}
`

func TestSourceMultiCow(t *testing.T) {
	// Create a base file
	cowDataBase := make([]byte, 4*1024)
	_, err := rand.Read(cowDataBase)
	assert.NoError(t, err)

	expectedData1 := make([]byte, len(cowDataBase))
	expectedData2 := make([]byte, len(cowDataBase))

	copy(expectedData1, cowDataBase)
	copy(expectedData2, cowDataBase)

	err = os.WriteFile("./testdata/testfile_base", cowDataBase, 0666)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		os.Remove("./testdata/testfile_base")
	})

	// Cow1, and do some writes
	s1 := new(config.SiloSchema)
	err = s1.Decode([]byte(testCow1Schema))
	assert.NoError(t, err)
	devs1, err := NewDevices(s1.Device)
	assert.NoError(t, err)
	t.Cleanup(func() {
		os.Remove("./testdata/testfile_cow1")
		os.Remove("./testdata/cow1_state")
	})

	assert.Equal(t, 1, len(devs1))

	// Write a couple of blocks
	buffer := []byte("cow1 testing 1 2 3")
	for _, offset := range []int64{400, 1400} {
		n, err := devs1["TestCow"].Provider.WriteAt(buffer, offset)
		assert.NoError(t, err)
		assert.Equal(t, len(buffer), n)
		copy(expectedData1[offset:], buffer)
		copy(expectedData2[offset:], buffer)
	}

	devs1["TestCow"].Provider.Close()

	// Now create / open a second cow layer on top

	s2 := new(config.SiloSchema)
	err = s2.Decode([]byte(testCow2Schema))
	assert.NoError(t, err)
	devs2, err := NewDevices(s2.Device)
	assert.NoError(t, err)
	t.Cleanup(func() {
		os.Remove("./testdata/testfile_cow2")
		os.Remove("./testdata/cow2_state")
	})

	assert.Equal(t, 1, len(devs2))

	// Now any writes should go to cow2...

	// Overwrite a block from the base, and one from cow1
	buffer2 := []byte("cow2")
	for _, offset := range []int64{400, 2400} {
		n, err := devs2["TestCow"].Provider.WriteAt(buffer2, offset)
		assert.NoError(t, err)
		assert.Equal(t, len(buffer2), n)
		copy(expectedData2[offset:], buffer2)
	}

	// Now check things are as we expect...

	// BASE - random data
	// COW1 - Writes to offsets 400 and 1400
	// COW2 - Writes to offsets 400 and 2400

	allData := make([]byte, len(cowDataBase))
	n, err := devs2["TestCow"].Provider.ReadAt(allData, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(allData), n)

	// Check the data is all as we expect from this viewpoint (base/cow1/cow2)

	assert.Equal(t, expectedData2, allData)
	devs2["TestCow"].Provider.Close()

	// Lastly, check the data is all as we expect from the lower layer (base/cow1)

	s3 := new(config.SiloSchema)
	err = s3.Decode([]byte(testCow1Schema))
	assert.NoError(t, err)
	devs3, err := NewDevices(s3.Device)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(devs3))

	allData3 := make([]byte, len(cowDataBase))
	n, err = devs3["TestCow"].Provider.ReadAt(allData3, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(allData3), n)

	// Check the data is all as we expect from this viewpoint (base/cow1)
	assert.Equal(t, expectedData1, allData3)
	devs3["TestCow"].Provider.Close()

}
