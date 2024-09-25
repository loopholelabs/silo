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
	"sync/atomic"
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
var connect_addr string

// Should we expose each device as an nbd device?
var connect_expose_dev bool

// Should we also mount the devices
var connect_mount_dev bool

var connect_progress bool

var connect_sync_conf bool
var connect_sync_endpoint string
var connect_sync_access string
var connect_sync_secret string
var connect_sync_bucket string

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

	cmdConnect.Flags().BoolVar(&connect_sync_conf, "s3sync", false, "Sync S3")
	cmdConnect.Flags().StringVar(&connect_sync_endpoint, "s3endpoint", "", "Sync S3 endpoint")
	cmdConnect.Flags().StringVar(&connect_sync_access, "s3access", "", "Sync S3 access token")
	cmdConnect.Flags().StringVar(&connect_sync_secret, "s3secret", "", "Sync S3 secret token")
	cmdConnect.Flags().StringVar(&connect_sync_bucket, "s3bucket", "", "Sync S3 bucket")
}

type block_info struct {
	block uint
	data  []byte
	hash  [32]byte
}

var s3Data map[string][]*block_info
var s3DataLock sync.Mutex

var metric_s3_blocks_retrieved uint64
var metric_s3_blocks_used uint64

/**
 * Connect to a silo source and stream whatever devices are available.
 *
 */
func runConnect(ccmd *cobra.Command, args []string) {
	if connect_sync_conf {
		s3Data = make(map[string][]*block_info)
	}

	if connect_progress {
		dst_progress = mpb.New(
			mpb.WithOutput(color.Output),
			mpb.WithAutoRefresh(),
		)

		dst_bars = make([]*mpb.Bar, 0)
	}

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

	// Show some info...
	s3_retrieved := atomic.LoadUint64(&metric_s3_blocks_retrieved)
	s3_used := atomic.LoadUint64(&metric_s3_blocks_used)
	metrics := pro.Metrics()

	fmt.Printf("S3 retrieved=%d used=%d\n", s3_retrieved, s3_used)
	fmt.Printf("Protocol recv packets=%d bytes=%d\n", metrics.Recv_packets, metrics.Recv_bytes)
	fmt.Printf("Protocol sent packets=%d bytes=%d\n", metrics.Sent_packets, metrics.Sent_bytes)

	fmt.Printf("\nMigrations completed. Please ctrl-c if you want to shut down, or wait an hour :)\n")

	// We should pause here, to allow the user to do things with the devices
	time.Sleep(10 * time.Hour)

	// Shutdown any storage exposed as devices
	for _, e := range dst_exposed {
		_ = dst_device_shutdown(e)
	}
}

// Handle a new incoming device. This is called when a packet is received for a device we haven't heard about before.
func handleIncomingDevice(ctx context.Context, pro protocol.Protocol, dev uint32) {
	var destStorage storage.StorageProvider
	var destWaitingLocal *waitingcache.WaitingCacheLocal
	var destWaitingRemote *waitingcache.WaitingCacheRemote
	var destMonitorStorage *modules.Hooks
	var dest *protocol.FromProtocol

	var dev_schema *config.DeviceSchema
	var dev_info *packets.DevInfo

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
		//		fmt.Printf("= %d = Received DevInfo name=%s size=%d blocksize=%d schema=%s\n", dev, di.Name, di.Size, di.Block_size, di.Schema)
		dev_info = di

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
		_ = conf.Decode(di.Schema)

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

		// FIXME... At the moment we're doing it here. We should do it somewhere else...
		go func() {
			// Send a list of hashes we already have to the source...
			hashes := make(map[uint][32]byte, 0)

			s3DataLock.Lock()
			h, ok := s3Data[di.Name]
			if ok {
				for _, bi := range h {
					if bi != nil {
						hashes[bi.block] = bi.hash
					}
				}
			}
			s3DataLock.Unlock()

			fmt.Printf("Sending list of hashes... %d\n", len(hashes))
			err = dest.SendHashes(hashes)
			if err != nil {
				panic(err)
			}

		}()

		return destWaitingRemote
	}

	dest = protocol.NewFromProtocol(ctx, dev, storageFactory, pro)

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
		_ = dest.HandleWriteAtHash(func(offset int64, length int64, hash []byte) []byte {
			block := uint(offset / int64(dev_info.Block_size))
			// Lookup the data based on a hash...
			s3DataLock.Lock()
			// dev_name should be set and usable...
			bi := s3Data[dev_info.Name][block]
			// TODO: Check the hash matches, although we could just trust serve is sane.

			s3DataLock.Unlock()

			atomic.AddUint64(&metric_s3_blocks_used, 1)

			return bi.data
		})
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

				// We completed the migration, but we should wait for handlers to finish before we ok things...
				//				fmt.Printf("Completed, now wait for handlers...\n")
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
		handler_wg.Done()
	}()

	handler_wg.Add(1)
	go func() {
		if connect_sync_conf {
			s3Storage, err := sources.NewS3Storage(connect_sync_endpoint, connect_sync_access, connect_sync_secret, connect_sync_bucket, dev_info.Name,
				uint64(dev_info.Size),
				int(dev_info.Block_size))
			if err != nil {
				panic(err)
			}

			num_blocks := (int(dev_info.Size) + int(dev_info.Block_size) - 1) / int(dev_info.Block_size)

			s3DataLock.Lock()
			s3Data[dev_info.Name] = make([]*block_info, num_blocks)
			s3DataLock.Unlock()

			_ = dest.HandleAlternateSources(func(sources []packets.AlternateSource) {
				for _, src := range sources {
					grab_from_s3(src, dest, dev_info, s3Storage)
				}
			})
		} else {
			_ = dest.HandleAlternateSources(func(sources []packets.AlternateSource) {
				// Do nothing about the alternate sources
			})
		}
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
		//		fmt.Printf("\n\n\np.Init returned %v\n\n\n", err)
		return nil, err
	}

	device := p.Device()
	//	fmt.Printf("* Device ready on /dev/%s\n", device)

	// We could also mount the device, but we should do so inside a goroutine, so that it doesn't block things...
	if connect_mount_dev {
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

/**
 * Grab a block from S3, and verify with the source that we have it.
 *
 */
func grab_from_s3(source packets.AlternateSource, dest *protocol.FromProtocol, dev_info *packets.DevInfo, s3Storage *sources.S3Storage) {
	block_no := uint(source.Offset) / uint(dev_info.Block_size)
	fmt.Printf("AlternateSource for block offset=%d length=%d block=%d %s\n", source.Offset, source.Length, block_no, source.Location)

	// Grab the block from S3 here, and send the hash once we have it.
	go func() {
		data := make([]byte, source.Length)
		_, err := s3Storage.ReadAt(data, source.Offset)
		if err == nil {
			hashes := make(map[uint][sha256.Size]byte)
			// Fill in the hash for the block that we have just got...
			hashes[block_no] = sha256.Sum256(data)
			err := dest.SendHashes(hashes)
			if err != nil {
				fmt.Printf("Error sending hash %v\n", err)
			}
			s3DataLock.Lock()
			s3Data[dev_info.Name][block_no] = &block_info{
				block: block_no,
				data:  data,
				hash:  hashes[block_no],
			}
			s3DataLock.Unlock()

			atomic.AddUint64(&metric_s3_blocks_retrieved, 1)
		} else {
			fmt.Printf("Error getting data from S3 %v\n", err)
		}
	}()
}
