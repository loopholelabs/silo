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

	_, err = s.Encode()
	assert.NoError(t, err)
	// TODO: Check data is as expected
}
