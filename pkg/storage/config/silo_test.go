package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSiloConfig(t *testing.T) {

	schema := `device Disk0 {
		size = "1G"
		expose = true
		system = "memory"
	}
	
	device Disk1 {
		size = "2M"
		system = "memory"
	}
	
	device Disk2 {
		size = "100M"
		system = "memory"
	}
	
	device Disk3 {
		size = "1234567"
		system = "memory"
	}
	
	device Memory0 {
		size = "2G"
		system = "memory"
	}
	
	device Memory1 {
		size = "7M"
		system = "memory"
	}
	
	device Stuff {
		size = "900"
		system = "memory"
	}
	
	device Other {
		size = "72M"
		system = "memory"
	}
	`

	s := new(SiloSchema)
	err := s.Decode([]byte(schema))
	assert.NoError(t, err)

	// Check things look ok...
	assert.Equal(t, 8, len(s.Device))

	assert.Equal(t, int64(1024*1024*1024), s.Device[0].ByteSize())
	assert.Equal(t, int64(2*1024*1024), s.Device[1].ByteSize())
	assert.Equal(t, int64(100*1024*1024), s.Device[2].ByteSize())
	assert.Equal(t, int64(1234567), s.Device[3].ByteSize())

	_, err = s.Encode()
	assert.NoError(t, err)
	// TODO: Check data is as expected
}

func TestSiloConfigBlock(t *testing.T) {

	schema := `device Disk0 {
		size = "1G"
		expose = true
		system = "memory"
	}
	
	device Disk1 {
		size = "2M"
		system = "memory"
	}
	`

	s := new(SiloSchema)
	err := s.Decode([]byte(schema))
	assert.NoError(t, err)

	block0 := s.Device[0].EncodeAsBlock()

	ds := &SiloSchema{}
	err = ds.Decode(block0)
	assert.NoError(t, err)

	// Make sure it used the label
	assert.Equal(t, ds.Device[0].Name, s.Device[0].Name)
}
