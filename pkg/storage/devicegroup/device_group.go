package devicegroup

import (
	"time"

	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
)

const volatilityExpiry = 30 * time.Minute

type DeviceGroup struct {
	devices []*DeviceInformation
}

type DeviceInformation struct {
	schema      *config.DeviceSchema
	prov        storage.Provider
	exp         storage.ExposedStorage
	volatility  *volatilitymonitor.VolatilityMonitor
	dirtyLocal  *dirtytracker.Local
	dirtyRemote *dirtytracker.Remote
}

func New(ds []*config.DeviceSchema, log types.Logger, met metrics.SiloMetrics) (*DeviceGroup, error) {
	dg := &DeviceGroup{
		devices: make([]*DeviceInformation, 0),
	}

	for _, s := range ds {
		prov, exp, err := device.NewDeviceWithLoggingMetrics(s, log, met)
		if err != nil {
			// We should try to close / shutdown any successful devices we created here...
			// But it's likely to be critical.
			for _, d := range dg.devices {
				d.exp.Shutdown()
				d.prov.Close()
			}
			return nil, err
		}

		mlocal := modules.NewMetrics(prov)
		dirtyLocal, dirtyRemote := dirtytracker.NewDirtyTracker(mlocal, int(s.ByteBlockSize()))
		vmonitor := volatilitymonitor.NewVolatilityMonitor(dirtyLocal, int(s.ByteBlockSize()), volatilityExpiry)
		vmonitor.AddAll()
		exp.SetProvider(vmonitor)

		// Add to metrics if given.
		if met != nil {
			met.AddMetrics(s.Name, mlocal)
			met.AddNBD(s.Name, exp.(*expose.ExposedStorageNBDNL))
			met.AddDirtyTracker(s.Name, dirtyRemote)
			met.AddVolatilityMonitor(s.Name, vmonitor)
		}

		dg.devices = append(dg.devices, &DeviceInformation{
			schema:      s,
			prov:        prov,
			exp:         exp,
			volatility:  vmonitor,
			dirtyLocal:  dirtyLocal,
			dirtyRemote: dirtyRemote,
		})
	}
	return dg, nil
}
