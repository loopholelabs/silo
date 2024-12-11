package devicegroup

import (
	"fmt"

	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

func NewFromProtocol(pro protocol.Protocol, log types.Logger, met metrics.SiloMetrics) (*DeviceGroup, error) {
	// This is our control channel, and we're expecting a DeviceGroupInfo
	_, dgData, err := pro.WaitForCommand(0, packets.CommandDeviceGroupInfo)
	if err != nil {
		return nil, err
	}
	dgi, err := packets.DecodeDeviceGroupInfo(dgData)
	if err != nil {
		return nil, err
	}

	fmt.Printf("DeviceGroupInfo %v\n", dgi)
	/*
		for index, di := range dgi.Devices {
			destStorageFactory := func(di *packets.DevInfo) storage.Provider {
				store := sources.NewMemoryStorage(int(di.Size))
				incomingLock.Lock()
				incomingProviders[uint32(index)] = store
				incomingLock.Unlock()
				return store
			}

			from := protocol.NewFromProtocol(ctx, uint32(index), destStorageFactory, prDest)
			err = from.SetDevInfo(di)
			assert.NoError(t, err)
			go func() {
				err := from.HandleReadAt()
				assert.ErrorIs(t, err, context.Canceled)
			}()
			go func() {
				err := from.HandleWriteAt()
				assert.ErrorIs(t, err, context.Canceled)
			}()
			go func() {
				err := from.HandleDirtyList(func(_ []uint) {
				})
				assert.ErrorIs(t, err, context.Canceled)
			}()
		}
	*/
	return nil, nil
}
