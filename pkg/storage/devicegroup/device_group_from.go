package devicegroup

import (
	"context"
	"errors"

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
	eventHandler func(e *packets.Event),
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

	devices := make([]*config.DeviceSchema, len(dgi.Devices))

	// First create the devices we need using the schemas sent...
	for index, di := range dgi.Devices {
		// We may want to tweak schemas here eg autoStart = false on sync. Or modify pathnames.
		schema := di.Schema
		if tweakDeviceSchema != nil {
			schema = tweakDeviceSchema(index-1, di.Name, schema)
		}
		ds, err := config.DecodeDeviceFromBlock(schema)
		if err != nil {
			return nil, err
		}
		devices[index-1] = ds
	}

	dg, err := NewFromSchema(devices, log, met)
	if err != nil {
		return nil, err
	}

	dg.controlProtocol = pro

	dg.incomingDevicesWg.Add(len(dg.devices))

	// We need to create the FromProtocol for each device, and associated goroutines here.
	for index, di := range dgi.Devices {
		dev := index - 1
		d := dg.devices[dev]
		d.EventHandler = eventHandler

		destStorageFactory := func(di *packets.DevInfo) storage.Provider {
			d.WaitingCacheLocal, d.WaitingCacheRemote = waitingcache.NewWaitingCacheWithLogger(d.Prov, int(di.BlockSize), dg.log)

			if d.Exp != nil {
				d.Exp.SetProvider(d.WaitingCacheLocal)
			}

			return d.WaitingCacheRemote
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
				d.WaitingCacheLocal.DirtyBlocks(dirtyBlocks)
			})
		}()
		go func() {
			from.HandleEvent(func(p *packets.Event) {
				if p.Type == packets.EventCompleted {
					dg.incomingDevicesWg.Done()
				}
				if d.EventHandler != nil {
					d.EventHandler(p)
				}
			})
		}()
	}

	return dg, nil
}

// Wait for completion events from all devices here.
func (dg *DeviceGroup) WaitForCompletion() {
	dg.incomingDevicesWg.Wait()
}

func (dg *DeviceGroup) HandleCustomData(cb func(customData []byte)) error {
	for {
		// This is our control channel, and we're expecting a DeviceGroupInfo
		id, evData, err := dg.controlProtocol.WaitForCommand(0, packets.CommandEvent)
		if err != nil {
			return err
		}
		ev, err := packets.DecodeEvent(evData)
		if err != nil {
			return err
		}

		if ev.Type != packets.EventCustom || ev.CustomType != 0 {
			return errors.New("unexpected event")
		}

		cb(ev.CustomPayload)

		// Reply with ack
		eack := packets.EncodeEventResponse()
		_, err = dg.controlProtocol.SendPacket(0, id, eack, protocol.UrgencyUrgent)
		if err != nil {
			return err
		}
	}
}
