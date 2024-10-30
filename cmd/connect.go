package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/integrity"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
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
}

/**
 * Connect to a silo source and stream whatever devices are available.
 *
 */
func runConnect(ccmd *cobra.Command, args []string) {
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

	proto_ctx, protoCancelfn := context.WithCancel(context.TODO())

	pro := protocol.NewProtocolRW(proto_ctx, []io.Reader{con}, []io.Writer{con}, handleIncomingDevice)

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

	dstWG.Wait() // Wait until the migrations have completed...

	if connectProgress {
		dstProgress.Wait()
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
func handleIncomingDevice(ctx context.Context, pro protocol.Protocol, dev uint32) {
	var destStorage storage.StorageProvider
	var destWaitingLocal *waitingcache.WaitingCacheLocal
	var destWaitingRemote *waitingcache.WaitingCacheRemote
	var destMonitorStorage *modules.Hooks
	var dest *protocol.FromProtocol

	var devSchema *config.DeviceSchema

	var bar *mpb.Bar

	var blockSize uint

	var statusString = " "
	var statusVerify = " "
	var statusExposed = "     "

	if !dstWGFirst {
		// We have a new migration to deal with
		dstWG.Add(1)
	}
	dstWGFirst = false

	// This is a storage factory which will be called when we recive DevInfo.
	storageFactory := func(di *packets.DevInfo) storage.StorageProvider {
		//		fmt.Printf("= %d = Received DevInfo name=%s size=%d blocksize=%d schema=%s\n", dev, di.Name, di.Size, di.Block_size, di.Schema)

		// Decode the schema
		devSchema = &config.DeviceSchema{}
		err := devSchema.Decode(di.Schema)
		if err != nil {
			panic(err)
		}

		blockSize = uint(di.BlockSize)

		statusFn := func(s decor.Statistics) string {
			return statusString + statusVerify
		}

		if connectProgress {
			bar = dstProgress.AddBar(int64(di.Size),
				mpb.PrependDecorators(
					decor.Name(di.Name, decor.WCSyncSpaceR),
					decor.Name(" "),
					decor.Any(func(s decor.Statistics) string { return statusExposed }, decor.WC{W: 4}),
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
		cr := func(i int, s int) (storage.StorageProvider, error) {
			return sources.NewMemoryStorage(s), nil
		}
		// Setup some sharded memory storage (for concurrent write speed)
		shard_size := di.Size
		if di.Size > 64*1024 {
			shard_size = di.Size / 1024
		}

		destStorage, err = modules.NewShardedStorage(int(di.Size), int(shard_size), cr)
		if err != nil {
			panic(err) // FIXME
		}

		destMonitorStorage = modules.NewHooks(destStorage)

		if connectProgress {
			lastValue := uint64(0)
			lastTime := time.Now()

			destMonitorStorage.PostWrite = func(buffer []byte, offset int64, n int, err error) (int, error) {
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
			if e.Type == packets.EventPostLock {
				statusString = "L" //red.Sprintf("L")
			} else if e.Type == packets.EventPreLock {
				statusString = "l" //red.Sprintf("l")
			} else if e.Type == packets.EventPostUnlock {
				statusString = "U" //green.Sprintf("U")
			} else if e.Type == packets.EventPreUnlock {
				statusString = "u" //green.Sprintf("u")
			}
			//fmt.Printf("= %d = Event %s\n", dev, protocol.EventsByType[e.Type])
			// Check we have all data...
			if e.Type == packets.EventCompleted {

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
			//fmt.Printf("[%d] Got %d hashes...\n", dev, len(hashes))
			if len(hashes) > 0 {
				in := integrity.NewIntegrityChecker(int64(destStorage.Size()), int(blockSize))
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
func dstDeviceSetup(prov storage.StorageProvider) (storage.ExposedStorage, error) {
	p := expose.NewExposedStorageNBDNL(prov, 1, 0, prov.Size(), 4096, true)
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
