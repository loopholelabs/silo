package memory

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaps(t *testing.T) {
	myPID := os.Getpid()

	maps, err := GetMaps(myPID)
	assert.NoError(t, err)

	// Make sure maps is right...

	for l, v := range maps {
		fmt.Printf("%d | %v\n", l, v)
	}
}
