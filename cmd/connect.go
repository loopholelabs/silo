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
	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
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
var connect_addr string

// Should we expose each device as an nbd device?
var connect_expose_dev bool

// Should we also mount the devices
var connect_mount_dev bool

var connect_progress bool

// List of ExposedStorage so they can be cleaned up on exit.
var dst_exposed []storage.ExposedStorage

var dst_progress *mpb.Progress
var dst_bars []*mpb.Bar
var dst_wg sync.WaitGroup
var dst_wg_first bool

func init() {
	rootCmd.AddCommand(cmdConnect)
	cmdConnect.Flags().StringVarP(&connect_addr, "addr", "a", "localhost:5170", "Address to serve from")
	cmdConnect.Flags().BoolVarP(&connect_expose_dev, "expose", "e", false, "Expose as an nbd devices")
	cmdConnect.Flags().BoolVarP(&connect_mount_dev, "mount", "m", false, "Mount the nbd devices")
	cmdConnect.Flags().BoolVarP(&connect_progress, "progress", "p", false, "Show progress")
}

/**
 * Connect to a silo source and stream whatever devices are available.
 *
 */
func runConnect(ccmd *cobra.Command, args []string) {
	if connect_progress {
		dst_progress = mpb.New(
			mpb.WithOutput(color.Output),
			mpb.WithAutoRefresh(),
		)

		dst_bars = make([]*mpb.Bar, 0)
	}

	fmt.Printf("Starting userfaultd\n")
	handle_page_faults()

	fmt.Printf("Starting silo connect from source %s\n", connect_addr)

	dst_exposed = make([]storage.ExposedStorage, 0)

	// Handle shutdown gracefully to disconnect any exposed devices correctly.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		for _, e := range dst_exposed {
			_ = dst_device_shutdown(e)
		}
		os.Exit(1)
	}()

	// Connect to the source
	fmt.Printf("Dialing source at %s...\n", connect_addr)
	con, err := net.Dial("tcp", connect_addr)
	if err != nil {
		panic("Error connecting to source")
	}

	// Wrap the connection in a protocol, and handle incoming devices
	dst_wg_first = true
	dst_wg.Add(1) // We need to at least wait for one to complete.

	proto_ctx, proto_cancelfn := context.WithCancel(context.TODO())

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
		proto_cancelfn()
	}()

	dst_wg.Wait() // Wait until the migrations have completed...

	if connect_progress {
		dst_progress.Wait()
	}

	fmt.Printf("\nMigrations completed. Please ctrl-c if you want to shut down, or wait an hour :)\n")

	// We should pause here, to allow the user to do things with the devices
	time.Sleep(10 * time.Hour)

	// Shutdown any storage exposed as devices
	for _, e := range dst_exposed {
		_ = dst_device_shutdown(e)
	}
}

// Handle a new incoming device. This is called when a packet is received for a device we haven't heard about before.
func handleIncomingDevice(pro protocol.Protocol, dev uint32) {
	var destStorage storage.StorageProvider
	var destWaitingLocal *waitingcache.WaitingCacheLocal
	var destWaitingRemote *waitingcache.WaitingCacheRemote
	var destMonitorStorage *modules.Hooks
	var dest *protocol.FromProtocol

	var mapped *modules.MappedStorage
	var dev_schema *config.DeviceSchema

	var bar *mpb.Bar

	var blockSize uint

	var statusString = " "
	var statusVerify = " "
	var statusExposed = "     "

	if !dst_wg_first {
		// We have a new migration to deal with
		dst_wg.Add(1)
	}
	dst_wg_first = false

	// This is a storage factory which will be called when we recive DevInfo.
	storageFactory := func(di *packets.DevInfo) storage.StorageProvider {
		fmt.Printf("= %d = Received DevInfo name=%s size=%d blocksize=%d schema=%s\n", dev, di.Name, di.Size, di.Block_size, di.Schema)

		// Decode the schema
		dev_schema = &config.DeviceSchema{}
		err := dev_schema.Decode(di.Schema)
		if err != nil {
			panic(err)
		}

		blockSize = uint(di.Block_size)

		statusFn := func(s decor.Statistics) string {
			return statusString + statusVerify
		}

		if connect_progress {
			bar = dst_progress.AddBar(int64(di.Size),
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

			dst_bars = append(dst_bars, bar)
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

		if connect_progress {
			last_value := uint64(0)
			last_time := time.Now()

			destMonitorStorage.Post_write = func(buffer []byte, offset int64, n int, err error) (int, error) {
				// Update the progress bar
				available, total := destWaitingLocal.Availability()
				v := uint64(available) * di.Size / uint64(total)
				bar.SetCurrent(int64(v))
				bar.EwmaIncrInt64(int64(v-last_value), time.Since(last_time))
				last_time = time.Now()
				last_value = v

				return n, err
			}
		}

		// Use a WaitingCache which will wait for migration blocks, send priorities etc
		// A WaitingCache has two ends - local and remote.
		destWaitingLocal, destWaitingRemote = waitingcache.NewWaitingCache(destMonitorStorage, int(di.Block_size))

		// Connect the waitingCache to the FromProtocol.
		// Note that since these are hints, errors don't matter too much.
		destWaitingLocal.NeedAt = func(offset int64, length int32) {
			_ = dest.NeedAt(offset, length)
		}

		destWaitingLocal.DontNeedAt = func(offset int64, length int32) {
			_ = dest.DontNeedAt(offset, length)
		}

		conf := &config.DeviceSchema{}
		conf.Decode(di.Schema)

		if conf.PageServerPID != 0 {
			fmt.Printf("Setting up mapped storage\n")
			mapped = modules.NewMappedStorage(destWaitingLocal, os.Getpagesize())
		}

		// Expose this storage as a device if requested
		if connect_expose_dev {
			p, err := dst_device_setup(destWaitingLocal)
			if err != nil {
				fmt.Printf("= %d = Error during setup (expose nbd) %v\n", dev, err)
			} else {
				statusExposed = p.Device()
				dst_exposed = append(dst_exposed, p)
			}
		}
		return destWaitingRemote
	}

	dest = protocol.NewFromProtocol(dev, storageFactory, pro)

	var handler_wg sync.WaitGroup

	handler_wg.Add(1)
	go func() {
		_ = dest.HandleReadAt()
		handler_wg.Done()
	}()
	handler_wg.Add(1)
	go func() {
		_ = dest.HandleWriteAt()
		handler_wg.Done()
	}()
	handler_wg.Add(1)
	go func() {
		_ = dest.HandleDevInfo()
		handler_wg.Done()
	}()
	handler_wg.Add(1)
	go func() {
		writer := func(offset int64, data []byte, idmap map[uint64]uint64) error {
			mapped.AppendMap(idmap)
			_, err := destWaitingRemote.WriteAt(data, offset)
			return err
		}
		_ = dest.HandleWriteAtWithMap(writer)
		handler_wg.Done()
	}()

	handler_wg.Add(1)
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
				if mapped != nil {
					// If it's a mapped type, add it to the userfault handler
					userfault_add(uint64(dev_schema.PageServerPID), mapped)
				}

				// We completed the migration, but we should wait for handlers to finish before we ok things...
				fmt.Printf("Completed, now wait for handlers...\n")
				go func() {
					handler_wg.Wait()
					dst_wg.Done()
				}()
				//			available, total := destWaitingLocal.Availability()
				//			fmt.Printf("= %d = Availability (%d/%d)\n", dev, available, total)
				// Set bar to completed
				if connect_progress {
					bar.SetCurrent(int64(destWaitingLocal.Size()))
				}
			}
		})
		handler_wg.Done()
	}()

	handler_wg.Add(1)
	go func() {
		_ = dest.HandleHashes(func(hashes map[uint][sha256.Size]byte) {
			//fmt.Printf("[%d] Got %d hashes...\n", dev, len(hashes))
			if len(hashes) > 0 {
				in := integrity.NewIntegrityChecker(int64(destStorage.Size()), int(blockSize))
				in.SetHashes(hashes)
				correct, err := in.Check(destStorage)
				//if err != nil {
				//	panic(err)
				//}
				fmt.Printf("[%d] Verification result %t %v\n", dev, correct, err)
				if correct {
					statusVerify = "\u2611"
				} else {
					statusVerify = "\u2612"
				}
			}
		})
		handler_wg.Done()
	}()

	// Handle dirty list by invalidating local waiting cache
	handler_wg.Add(1)
	go func() {
		_ = dest.HandleDirtyList(func(dirty []uint) {
			//	fmt.Printf("= %d = LIST OF DIRTY BLOCKS %v\n", dev, dirty)
			destWaitingLocal.DirtyBlocks(dirty)
		})
		handler_wg.Done()
	}()
}

// Called to setup an exposed storage device
func dst_device_setup(prov storage.StorageProvider) (storage.ExposedStorage, error) {
	p := expose.NewExposedStorageNBDNL(prov, 1, 0, prov.Size(), 4096, true)
	var err error

	err = p.Init()
	if err != nil {
		fmt.Printf("\n\n\np.Init returned %v\n\n\n", err)
		return nil, err
	}

	device := p.Device()
	//	fmt.Printf("* Device ready on /dev/%s\n", device)

	// We could also mount the device, but we should do so inside a goroutine, so that it doesn't block things...
	if connect_mount_dev {
		err = os.Mkdir(fmt.Sprintf("/mnt/mount%s", device), 0600)
		if err != nil {
			return nil, fmt.Errorf("Error mkdir %v", err)
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
func dst_device_shutdown(p storage.ExposedStorage) error {
	device := p.Device()

	fmt.Printf("Shutdown %s\n", device)
	if connect_mount_dev {
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

var userfault_map map[uint64]*modules.MappedStorage

func userfault_add(pid uint64, mapped *modules.MappedStorage) {
	fmt.Printf("Add device to userfault availability pid [%d]... size %d", pid, mapped.Size())

	userfault_map[pid] = mapped
}

func handle_page_faults() {
	userfault_map = make(map[uint64]*modules.MappedStorage)

	faults_served := make(map[uint64]bool)
	// TODO: Mutex etc

	var err error
	var uf *criu.UserFaultHandler

	num_faults := 0
	num_syscalls := 0

	fault := func(pid uint32, addr uint64, pending []uint64) error {
		num_faults++
		// Look it up in page_data...
		maps, ok := userfault_map[uint64(pid)]

		fmt.Printf("Fault for %d address %x, with pending %v\n", pid, addr, pending)

		_, served := faults_served[uint64(pid)]
		if served {
			return nil
		}
		faults_served[uint64(pid)] = true

		if !ok {
			panic("We do not have such a page for that pid!")
		}

		// Try to send all lazy data...
		go func() {
			// TRY TO SEND ALL OUR LAZY DATA IN ONE SHOT (atm)...

			addresses := maps.GetBlockAddresses()
			max_size := uint64(256 * 1024)

			ranges := maps.GetRegions(addresses, max_size)

			send_pages := make(map[uint64][]byte)

			// Read the data into memory so its ready to send in one shot...
			for a, l := range ranges {
				data := make([]byte, l)
				err := maps.ReadBlocks(a, data)
				if err != nil {
					panic(err)
				}
				send_pages[a] = data
			}

			ctime := time.Now()

			// Send all the data over...
			for addr, data := range send_pages {
				uf.WriteData(uint64(pid), addr, data)
				num_syscalls++
			}

			// We've sent everything, we can quit now!
			fmt.Printf("All Data Sent %dms faults=%d syscalls=%d\n", time.Since(ctime).Milliseconds(), num_faults, num_syscalls)

			err = uf.ClosePID(uint64(pid))
			fmt.Printf("Close uffd %v\n", err)
			if err != nil {
				panic(err)
			}

			// Allow more faults now if it's being reused...
			delete(faults_served, uint64(pid))
		}()

		return nil
	}

	uf, err = criu.NewUserFaultHandler("lazy-pages.socket", fault)
	if err != nil {
		panic(err)
	}

	// Handle any userfault stuff...
	go func() {
		err = uf.Handle()
		if err != nil {
			panic(err)
		}
	}()

}
