package devicegroup

import (
	"context"

	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
)

func NewFromProtocol(ctx context.Context,
	pro protocol.Protocol,
	tweakDeviceSchema func(index int, name string, schema string) string,
	log types.Logger,
	met metrics.SiloMetrics) (*DeviceGroup, error) {

	// This is our control channel, and we're expecting a DeviceGroupInfo
	_, dgData, err := pro.WaitForCommand(0, packets.CommandDeviceGroupInfo)
	if err != nil {
		return nil, err
	}
	dgi, err := packets.DecodeDeviceGroupInfo(dgData)
	if err != nil {
		return nil, err
	}

	devices := make([]*config.DeviceSchema, 0)

	// First create the devices we need using the schemas sent...
	for index, di := range dgi.Devices {
		ds := &config.DeviceSchema{}
		// We may want to tweak schemas here eg autoStart = false on sync. Or modify pathnames.
		schema := di.Schema
		if tweakDeviceSchema != nil {
			schema = tweakDeviceSchema(index-1, di.Name, schema)
		}
		err := ds.Decode(schema)
		if err != nil {
			return nil, err
		}
		devices = append(devices, ds)
	}

	dg, err := NewFromSchema(devices, log, met)
	if err != nil {
		return nil, err
	}

	dg.incomingDevicesWg.Add(len(dg.devices))

	// We need to create the FromProtocol for each device, and associated goroutines here.
	for index, di := range dgi.Devices {
		d := dg.devices[index-1]

		destStorageFactory := func(di *packets.DevInfo) storage.Provider {
			d.waitingCacheLocal, d.waitingCacheRemote = waitingcache.NewWaitingCacheWithLogger(d.prov, int(di.BlockSize), dg.log)

			if dg.devices[index-1].exp != nil {
				dg.devices[index-1].exp.SetProvider(d.waitingCacheLocal)
			}

			return d.waitingCacheRemote
		}

		from := protocol.NewFromProtocol(ctx, uint32(index), destStorageFactory, pro)
		err = from.SetDevInfo(di)
		if err != nil {
			return nil, err
		}
		go func() {
			_ = from.HandleReadAt()
		}()
		go func() {
			_ = from.HandleWriteAt()
		}()
		go func() {
			_ = from.HandleDirtyList(func(dirtyBlocks []uint) {
				// Tell the waitingCache about it
				d.waitingCacheLocal.DirtyBlocks(dirtyBlocks)
			})
		}()
		go func() {
			from.HandleEvent(func(p *packets.Event) {
				if p.Type == packets.EventCompleted {
					dg.incomingDevicesWg.Done()
				}
				// TODO: Pass events on to caller so they can be handled upstream
			})
		}()
	}

	return dg, nil
}

// Wait for completion events from all devices here.
func (dg *DeviceGroup) WaitForCompletion() {
	dg.incomingDevicesWg.Wait()
}
