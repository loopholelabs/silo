package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/loopholelabs/logging"
	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/integrity"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"

	"github.com/fatih/color"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

var (
	cmdConnect = &cobra.Command{
		Use:   "connect",
		Short: "Conndct and stream Silo devices.",
		Long:  `Connect to a Silo instance, and stream available devices.`,
		Run:   runConnect,
	}
)

// Address to connect to
var connectAddr string

// Should we expose each device as an nbd device?
var connectExposeDev bool

// Should we also mount the devices
var connectMountDev bool

var connectProgress bool

var connectDebug bool

var connectMetrics string

// List of ExposedStorage so they can be cleaned up on exit.
var dstExposed []storage.ExposedStorage

var dstProgress *mpb.Progress
var dstBars []*mpb.Bar
var dstWG sync.WaitGroup
var dstWGFirst bool

func init() {
	rootCmd.AddCommand(cmdConnect)
	cmdConnect.Flags().StringVarP(&connectAddr, "addr", "a", "localhost:5170", "Address to serve from")
	cmdConnect.Flags().BoolVarP(&connectExposeDev, "expose", "e", false, "Expose as an nbd devices")
	cmdConnect.Flags().BoolVarP(&connectMountDev, "mount", "m", false, "Mount the nbd devices")
	cmdConnect.Flags().BoolVarP(&connectProgress, "progress", "p", false, "Show progress")
	cmdConnect.Flags().BoolVarP(&connectDebug, "debug", "d", false, "Debug logging (trace)")
	cmdConnect.Flags().StringVarP(&connectMetrics, "metrics", "M", "", "Prom metrics address")
}

/**
 * Connect to a silo source and stream whatever devices are available.
 *
 */
func runConnect(_ *cobra.Command, _ []string) {
	var log types.RootLogger
	var reg *prometheus.Registry
	var siloMetrics *metrics.Metrics

	if connectDebug {
		log = logging.New(logging.Zerolog, "silo.connect", os.Stderr)
		log.SetLevel(types.TraceLevel)
	}

	if connectMetrics != "" {
		reg = prometheus.NewRegistry()

		siloMetrics = metrics.New(reg)

		// Add the default go metrics
		reg.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)

		http.Handle("/metrics", promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				// Opt into OpenMetrics to support exemplars.
				EnableOpenMetrics: true,
				// Pass custom registry
				Registry: reg,
			},
		))

		go http.ListenAndServe(connectMetrics, nil)
	}

	if connectProgress {
		dstProgress = mpb.New(
			mpb.WithOutput(color.Output),
			mpb.WithAutoRefresh(),
		)

		dstBars = make([]*mpb.Bar, 0)
	}

	fmt.Printf("Starting silo connect from source %s\n", connectAddr)

	dstExposed = make([]storage.ExposedStorage, 0)

	// Handle shutdown gracefully to disconnect any exposed devices correctly.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		for _, e := range dstExposed {
			_ = dstDeviceShutdown(e)
		}
		os.Exit(1)
	}()

	// Connect to the source
	fmt.Printf("Dialing source at %s...\n", connectAddr)
	con, err := net.Dial("tcp", connectAddr)
	if err != nil {
		panic("Error connecting to source")
	}

	// Wrap the connection in a protocol, and handle incoming devices
	dstWGFirst = true
	dstWG.Add(1) // We need to at least wait for one to complete.

	protoCtx, protoCancelfn := context.WithCancel(context.TODO())

	handleIncomingDevice := func(ctx context.Context, pro protocol.Protocol, dev uint32) {
		handleIncomingDeviceWithLogging(ctx, pro, dev, log, siloMetrics)
	}

	pro := protocol.NewRW(protoCtx, []io.Reader{con}, []io.Writer{con}, handleIncomingDevice)

	// Let the protocol do its thing.
	go func() {
		err = pro.Handle()
		if err != nil && err != io.EOF {
			fmt.Printf("Silo protocol error %v\n", err)
			return
		}
		// We should get an io.EOF here once the migrations have all completed.
		// We should cancel the context, to cancel anything that is waiting for packets.
		protoCancelfn()
	}()

	if siloMetrics != nil {
		siloMetrics.AddProtocol("protocol", pro)
	}

	dstWG.Wait() // Wait until the migrations have completed...

	if connectProgress {
		dstProgress.Wait()
	}

	if log != nil {
		metrics := pro.GetMetrics()
		log.Debug().
			Uint64("PacketsSent", metrics.PacketsSent).
			Uint64("DataSent", metrics.DataSent).
			Uint64("PacketsRecv", metrics.PacketsRecv).
			Uint64("DataRecv", metrics.DataRecv).
			Msg("protocol metrics")
	}

	fmt.Printf("\nMigrations completed. Please ctrl-c if you want to shut down, or wait an hour :)\n")

	// We should pause here, to allow the user to do things with the devices
	time.Sleep(10 * time.Hour)

	// Shutdown any storage exposed as devices
	for _, e := range dstExposed {
		_ = dstDeviceShutdown(e)
	}
}

// Handle a new incoming device. This is called when a packet is received for a device we haven't heard about before.
func handleIncomingDeviceWithLogging(ctx context.Context, pro protocol.Protocol, dev uint32, log types.RootLogger, met *metrics.Metrics) {
	var destStorage storage.Provider
	var destWaitingLocal *waitingcache.Local
	var destWaitingRemote *waitingcache.Remote
	var destMonitorStorage *modules.Hooks
	var dest *protocol.FromProtocol

	var devSchema *config.DeviceSchema

	var bar *mpb.Bar

	var blockSize uint
	var deviceName string

	var statusString = " "
	var statusVerify = " "
	var statusExposed = "     "

	if !dstWGFirst {
		// We have a new migration to deal with
		dstWG.Add(1)
	}
	dstWGFirst = false

	// This is a storage factory which will be called when we recive DevInfo.
	storageFactory := func(di *packets.DevInfo) storage.Provider {
		//		fmt.Printf("= %d = Received DevInfo name=%s size=%d blocksize=%d schema=%s\n", dev, di.Name, di.Size, di.Block_size, di.Schema)

		// Decode the schema
		devSchema = &config.DeviceSchema{}
		err := devSchema.Decode(di.Schema)
		if err != nil {
			panic(err)
		}

		blockSize = uint(di.BlockSize)
		deviceName = di.Name

		statusFn := func(_ decor.Statistics) string {
			return statusString + statusVerify
		}

		if connectProgress {
			bar = dstProgress.AddBar(int64(di.Size),
				mpb.PrependDecorators(
					decor.Name(di.Name, decor.WCSyncSpaceR),
					decor.Name(" "),
					decor.Any(func(_ decor.Statistics) string { return statusExposed }, decor.WC{W: 4}),
					decor.Name(" "),
					decor.CountersKiloByte("%d/%d", decor.WCSyncWidth),
				),
				mpb.AppendDecorators(
					decor.EwmaETA(decor.ET_STYLE_GO, 30),
					decor.Name(" "),
					decor.EwmaSpeed(decor.SizeB1024(0), "% .2f", 60, decor.WCSyncWidth),
					decor.OnComplete(decor.Percentage(decor.WC{W: 5}), "done"),
					decor.Name(" "),
					decor.Any(statusFn, decor.WC{W: 2}),
				),
			)

			dstBars = append(dstBars, bar)
		}

		// You can change this to use sources.NewFileStorage etc etc
		cr := func(_ int, s int) (storage.Provider, error) {
			return sources.NewMemoryStorage(s), nil
		}
		// Setup some sharded memory storage (for concurrent write speed)
		shardSize := di.Size
		if di.Size > 64*1024 {
			shardSize = di.Size / 1024
		}

		destStorage, err = modules.NewShardedStorage(int(di.Size), int(shardSize), cr)
		if err != nil {
			panic(err) // FIXME
		}

		destMonitorStorage = modules.NewHooks(destStorage)

		if connectProgress {
			lastValue := uint64(0)
			lastTime := time.Now()

			destMonitorStorage.PostWrite = func(_ []byte, _ int64, n int, err error) (int, error) {
				// Update the progress bar
				available, total := destWaitingLocal.Availability()
				v := uint64(available) * di.Size / uint64(total)
				bar.SetCurrent(int64(v))
				bar.EwmaIncrInt64(int64(v-lastValue), time.Since(lastTime))
				lastTime = time.Now()
				lastValue = v

				return n, err
			}
		}

		// Use a WaitingCache which will wait for migration blocks, send priorities etc
		// A WaitingCache has two ends - local and remote.
		destWaitingLocal, destWaitingRemote = waitingcache.NewWaitingCache(destMonitorStorage, int(di.BlockSize))

		// Connect the waitingCache to the FromProtocol.
		// Note that since these are hints, errors don't matter too much.
		destWaitingLocal.NeedAt = func(offset int64, length int32) {
			_ = dest.NeedAt(offset, length)
		}

		destWaitingLocal.DontNeedAt = func(offset int64, length int32) {
			_ = dest.DontNeedAt(offset, length)
		}

		conf := &config.DeviceSchema{}
		_ = conf.Decode(di.Schema)

		// Expose this storage as a device if requested
		if connectExposeDev {
			p, err := dstDeviceSetup(destWaitingLocal)
			if err != nil {
				fmt.Printf("= %d = Error during setup (expose nbd) %v\n", dev, err)
			} else {
				statusExposed = p.Device()
				dstExposed = append(dstExposed, p)
			}
		}
		return destWaitingRemote
	}

	dest = protocol.NewFromProtocol(ctx, dev, storageFactory, pro)

	if met != nil {
		met.AddFromProtocol(deviceName, dest)
	}

	var handlerWG sync.WaitGroup

	handlerWG.Add(1)
	go func() {
		_ = dest.HandleReadAt()
		handlerWG.Done()
	}()
	handlerWG.Add(1)
	go func() {
		_ = dest.HandleWriteAt()
		handlerWG.Done()
	}()
	handlerWG.Add(1)
	go func() {
		_ = dest.HandleDevInfo()
		handlerWG.Done()
	}()

	handlerWG.Add(1)
	// Handle events from the source
	go func() {
		_ = dest.HandleEvent(func(e *packets.Event) {
			switch e.Type {

			case packets.EventPostLock:
				statusString = "L" // red.Sprintf("L")
			case packets.EventPreLock:
				statusString = "l" // red.Sprintf("l")
			case packets.EventPostUnlock:
				statusString = "U" // green.Sprintf("U")
			case packets.EventPreUnlock:
				statusString = "u" // green.Sprintf("u")

				// fmt.Printf("= %d = Event %s\n", dev, protocol.EventsByType[e.Type])
				// Check we have all data...
			case packets.EventCompleted:

				if log != nil {
					m := destWaitingLocal.GetMetrics()
					log.Debug().
						Uint64("WaitForBlock", m.WaitForBlock).
						Uint64("WaitForBlockHadRemote", m.WaitForBlockHadRemote).
						Uint64("WaitForBlockHadLocal", m.WaitForBlockHadLocal).
						Uint64("WaitForBlockTimeMS", uint64(m.WaitForBlockTime.Milliseconds())).
						Uint64("WaitForBlockLock", m.WaitForBlockLock).
						Uint64("WaitForBlockLockDone", m.WaitForBlockLockDone).
						Uint64("MarkAvailableLocalBlock", m.MarkAvailableLocalBlock).
						Uint64("MarkAvailableRemoteBlock", m.MarkAvailableRemoteBlock).
						Uint64("AvailableLocal", m.AvailableLocal).
						Uint64("AvailableRemote", m.AvailableRemote).
						Str("name", deviceName).
						Msg("waitingCacheMetrics")

					fromMetrics := dest.GetMetrics()
					log.Debug().
						Uint64("RecvEvents", fromMetrics.RecvEvents).
						Uint64("RecvHashes", fromMetrics.RecvHashes).
						Uint64("RecvDevInfo", fromMetrics.RecvDevInfo).
						Uint64("RecvAltSources", fromMetrics.RecvAltSources).
						Uint64("RecvReadAt", fromMetrics.RecvReadAt).
						Uint64("RecvWriteAtHash", fromMetrics.RecvWriteAtHash).
						Uint64("RecvWriteAtComp", fromMetrics.RecvWriteAtComp).
						Uint64("RecvWriteAt", fromMetrics.RecvWriteAt).
						Uint64("RecvWriteAtWithMap", fromMetrics.RecvWriteAtWithMap).
						Uint64("RecvRemoveFromMap", fromMetrics.RecvRemoveFromMap).
						Uint64("RecvRemoveDev", fromMetrics.RecvRemoveDev).
						Uint64("RecvDirtyList", fromMetrics.RecvDirtyList).
						Uint64("SentNeedAt", fromMetrics.SentNeedAt).
						Uint64("SentDontNeedAt", fromMetrics.SentDontNeedAt).
						Str("name", deviceName).
						Msg("fromProtocolMetrics")
				}

				// We completed the migration, but we should wait for handlers to finish before we ok things...
				//				fmt.Printf("Completed, now wait for handlers...\n")
				go func() {
					handlerWG.Wait()
					dstWG.Done()
				}()
				//			available, total := destWaitingLocal.Availability()
				//			fmt.Printf("= %d = Availability (%d/%d)\n", dev, available, total)
				// Set bar to completed
				if connectProgress {
					bar.SetCurrent(int64(destWaitingLocal.Size()))
				}
			}
		})
		handlerWG.Done()
	}()

	handlerWG.Add(1)
	go func() {
		_ = dest.HandleHashes(func(hashes map[uint][sha256.Size]byte) {
			// fmt.Printf("[%d] Got %d hashes...\n", dev, len(hashes))
			if len(hashes) > 0 {
				in := integrity.NewChecker(int64(destStorage.Size()), int(blockSize))
				in.SetHashes(hashes)
				correct, err := in.Check(destStorage)
				if err != nil {
					panic(err)
				}
				//				fmt.Printf("[%d] Verification result %t %v\n", dev, correct, err)
				if correct {
					statusVerify = "\u2611"
				} else {
					statusVerify = "\u2612"
				}
			}
		})
		handlerWG.Done()
	}()

	// Handle dirty list by invalidating local waiting cache
	handlerWG.Add(1)
	go func() {
		_ = dest.HandleDirtyList(func(dirty []uint) {
			//	fmt.Printf("= %d = LIST OF DIRTY BLOCKS %v\n", dev, dirty)
			destWaitingLocal.DirtyBlocks(dirty)
		})
		handlerWG.Done()
	}()
}

// Called to setup an exposed storage device
func dstDeviceSetup(prov storage.Provider) (storage.ExposedStorage, error) {
	p := expose.NewExposedStorageNBDNL(prov, expose.DefaultConfig)
	var err error

	err = p.Init()
	if err != nil {
		//		fmt.Printf("\n\n\np.Init returned %v\n\n\n", err)
		return nil, err
	}

	device := p.Device()
	//	fmt.Printf("* Device ready on /dev/%s\n", device)

	// We could also mount the device, but we should do so inside a goroutine, so that it doesn't block things...
	if connectMountDev {
		err = os.Mkdir(fmt.Sprintf("/mnt/mount%s", device), 0600)
		if err != nil {
			return nil, fmt.Errorf("error mkdir %v", err)
		}

		go func() {
			//			fmt.Printf("Mounting device...")
			cmd := exec.Command("mount", "-r", fmt.Sprintf("/dev/%s", device), fmt.Sprintf("/mnt/mount%s", device))
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Could not mount device %v\n", err)
				return
			}
			//			fmt.Printf("* Device is mounted at /mnt/mount%s\n", device)
		}()
	}

	return p, nil
}

// Called to shutdown an exposed storage device
func dstDeviceShutdown(p storage.ExposedStorage) error {
	device := p.Device()

	fmt.Printf("Shutdown %s\n", device)
	if connectMountDev {
		cmd := exec.Command("umount", fmt.Sprintf("/dev/%s", device))
		err := cmd.Run()
		if err != nil {
			return err
		}
		err = os.Remove(fmt.Sprintf("/mnt/mount%s", device))
		if err != nil {
			return err
		}
	}

	err := p.Shutdown()
	if err != nil {
		return err
	}
	return nil
}
