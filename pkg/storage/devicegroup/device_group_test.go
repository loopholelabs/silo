package devicegroup

import (
	"context"
	"fmt"
	"os/user"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
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
		{
			Name:      "test2",
			Size:      "16m",
			System:    "file",
			BlockSize: "1m",
			Expose:    true,
			Location:  "testdev_test2",
		},
	}

	dg, err := New(ds, nil, nil)
	assert.NoError(t, err)

	err = dg.CloseAll()
	assert.NoError(t, err)
}

func TestDeviceGroupSendDevInfo(t *testing.T) {
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
		{
			Name:      "test2",
			Size:      "16m",
			System:    "file",
			BlockSize: "1m",
			Expose:    true,
			Location:  "testdev_test2",
		},
	}

	dg, err := New(ds, nil, nil)
	assert.NoError(t, err)

	pro := protocol.NewMockProtocol(context.TODO())

	err = dg.SendDevInfo(pro)
	assert.NoError(t, err)

	err = dg.CloseAll()
	assert.NoError(t, err)

	// Make sure they all got sent correctly...
	for index, r := range ds {
		_, data, err := pro.WaitForCommand(uint32(index), packets.CommandDevInfo)
		assert.NoError(t, err)

		di, err := packets.DecodeDevInfo(data)
		assert.NoError(t, err)

		assert.Equal(t, r.Name, di.Name)
		assert.Equal(t, uint64(r.ByteSize()), di.Size)
		assert.Equal(t, uint32(r.ByteBlockSize()), di.BlockSize)
		assert.Equal(t, string(r.Encode()), di.Schema)
	}
}
