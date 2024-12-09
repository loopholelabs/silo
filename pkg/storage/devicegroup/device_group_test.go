package devicegroup

import (
	"fmt"
	"os/user"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/stretchr/testify/assert"
)

func TestDeviceGroupBasic(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	ds := []*config.DeviceSchema{
		{
			Name:      "test1",
			Size:      "8m",
			System:    "file",
			BlockSize: "1m",
			Expose:    true,
			Location:  "testdev_test1",
		},
	}

	dg, err := New(ds, nil, nil)
	assert.NoError(t, err)

	err = dg.CloseAll()
	assert.NoError(t, err)
}
