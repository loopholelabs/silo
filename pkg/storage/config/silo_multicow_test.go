package config

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testDir = "testmulticow"

func TestMultiCowAddRemove(t *testing.T) {

	ds := &DeviceSchema{
		Name:      "test",
		Location:  "Base",
		System:    "file",
		Size:      "100",
		BlockSize: "10",
	}

	encodedStart := ds.Encode()

	overlay1 := &CowOverlay{
		Name:     "overlay1",
		System:   "file",
		Location: "overlay1",
		Shared:   true,
	}

	ds.AddOverlay(overlay1)

	encodedMid := ds.Encode()

	assert.NotEqual(t, encodedStart, encodedMid)

	overlay, err := ds.RemoveOverlay()
	assert.NoError(t, err)

	assert.Equal(t, overlay1.Name, overlay.Name)
	assert.Equal(t, overlay1.System, overlay.System)
	assert.Equal(t, overlay1.Location, overlay.Location)
	assert.Equal(t, overlay1.Shared, overlay.Shared)

	encodedEnd := ds.Encode()

	assert.Equal(t, encodedStart, encodedEnd)
}

func TestMultiCow(t *testing.T) {
	err := os.Mkdir(testDir, 0777)
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := os.RemoveAll(testDir)
		assert.NoError(t, err)
	})

	for n := 0; n < 3; n++ {
		err = os.WriteFile(path.Join(testDir, fmt.Sprintf("dev_%03d.overlay", n)), []byte("hi"), 0666)
		assert.NoError(t, err)
		err = os.WriteFile(path.Join(testDir, fmt.Sprintf("dev_%03d.state", n)), []byte("hi"), 0666)
		assert.NoError(t, err)
	}

	ds := &DeviceSchema{
		Name:      "test",
		Location:  "Base",
		System:    "file",
		Size:      "100",
		BlockSize: "10",
	}

	// Load the overlays from a dir
	err = ds.AddOverlays(testDir, "dev_", "sparsefile", true)
	assert.NoError(t, err)

	encoded := ds.Encode()
	assert.Equal(t, []byte(`size      = "100"
system    = "sparsefile"
blocksize = "10"
expose    = false
location  = "dev_002.overlay"

source "dev_002.state" {
  size      = "100"
  system    = "sparsefile"
  blocksize = "10"
  expose    = false
  location  = "dev_001.overlay"

  source "dev_001.state" {
    size      = "100"
    system    = "sparsefile"
    blocksize = "10"
    expose    = false
    location  = "dev_000.overlay"

    source "dev_000.state" {
      size         = "100"
      system       = "file"
      blocksize    = "10"
      expose       = false
      location     = "Base"
      sourcehashes = ""
      sourceshared = false
      loadbinlog   = ""
      binlog       = ""
      pid          = 0
    }

    sourcehashes = ""
    sourceshared = true
    loadbinlog   = ""
    binlog       = ""
    pid          = 0
  }

  sourcehashes = ""
  sourceshared = true
  loadbinlog   = ""
  binlog       = ""
  pid          = 0
}

sourcehashes = ""
sourceshared = true
loadbinlog   = ""
binlog       = ""
pid          = 0
`), encoded)

}
