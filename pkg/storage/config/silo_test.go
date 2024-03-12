package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSiloConfig(t *testing.T) {

	schema := `device Disk0 {
		size = "1G"
		expose = true
	}
	
	device Disk1 {
		size = "2M"
	}
	
	device Disk2 {
		size = "100M"
	}
	
	device Disk3 {
		size = "1234567"
	}
	
	device Memory0 {
		size = "2G"
	}
	
	device Memory1 {
		size = "7M"
	}
	
	device Stuff {
		size = "900"
	}
	
	device Other {
		size = "72M"
	}
	
	`

	s := new(SiloSchema)
	err := s.Decode([]byte(schema))
	assert.NoError(t, err)

	// Check things look ok...
	assert.Equal(t, 8, len(s.Device))

	_, err = s.Encode()
	assert.NoError(t, err)
	// TODO: Check data
}
