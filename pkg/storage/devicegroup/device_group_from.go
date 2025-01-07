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
	tweakDeviceSchema func(index int, name string, schema *config.DeviceSchema) *config.DeviceSchema,
	eventHandler func(e *packets.Event),
	customDataHandler func(data []byte),
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

	// Setup something to listen for custom data...
	handleCustomDataEvent := func() error {
		// This is our control channel, and we're expecting a DeviceGroupInfo
		id, evData, err := pro.WaitForCommand(0, packets.CommandEvent)
		if err != nil {
			return err
		}
		ev, err := packets.DecodeEvent(evData)
		if err != nil {
			return err
		}
		if ev.Type != packets.EventCustom || ev.CustomType != 0 {
			return err
		}

		if customDataHandler != nil {
			customDataHandler(ev.CustomPayload)
		}

		// Reply with ack
		eack := packets.EncodeEventResponse()
		_, err = pro.SendPacket(0, id, eack, protocol.UrgencyUrgent)
		if err != nil {
			return err
		}
		return nil
	}

	// Listen for custom data events
	go func() {
		for {
			err := handleCustomDataEvent()
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Debug().Err(err).Msg("handleCustomDataEvenet returned")
				return
			}
		}
	}()

	// First create the devices we need using the schemas sent...
	for index, di := range dgi.Devices {
		// We may want to tweak schemas here eg autoStart = false on sync. Or modify pathnames.
		ds, err := config.DecodeDeviceFromBlock(di.Schema)
		if err != nil {
			return nil, err
		}
		if tweakDeviceSchema != nil {
			ds = tweakDeviceSchema(index-1, di.Name, ds)
		}
		devices[index-1] = ds
	}

	dg, err := NewFromSchema(devices, log, met)
	if err != nil {
		return nil, err
	}

	dg.controlProtocol = pro
	dg.ctx = ctx

	dg.incomingDevicesCh = make(chan bool, len(dg.devices))

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
			err := from.HandleReadAt()
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Debug().Err(err).Msg("HandleReadAt returned")
			}
		}()
		go func() {
			err := from.HandleWriteAt()
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Debug().Err(err).Msg("HandleWriteAt returned")
			}
		}()
		go func() {
			err := from.HandleDirtyList(func(dirtyBlocks []uint) {
				// Tell the waitingCache about it
				d.WaitingCacheLocal.DirtyBlocks(dirtyBlocks)
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Debug().Err(err).Msg("HandleDirtyList returned")
			}
		}()
		go func() {
			err := from.HandleEvent(func(p *packets.Event) {
				if p.Type == packets.EventCompleted {
					dg.incomingDevicesCh <- true
				}
				if d.EventHandler != nil {
					d.EventHandler(p)
				}
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Debug().Err(err).Msg("HandleEvent returned")
			}
		}()
	}

	return dg, nil
}

// Wait for completion events from all devices here.
func (dg *DeviceGroup) WaitForCompletion() error {
	for range dg.devices {
		select {
		case <-dg.incomingDevicesCh:
		case <-dg.ctx.Done():
			return dg.ctx.Err()
		}
	}
	return nil
}
