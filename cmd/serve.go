package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

var (
	cmdServe = &cobra.Command{
		Use:   "serve",
		Short: "Start up serve",
		Long:  ``,
		Run:   runServe,
	}
)

var serve_addr string
var serve_conf string
var serve_progress bool
var serve_continuous bool
var serve_any_order bool
var serve_sync_s3 bool

var serve_sync_dirty_block_shift int
var serve_sync_endpoint string
var serve_sync_access string
var serve_sync_secret string
var serve_sync_bucket string
var serve_sync_block_max_age time.Duration
var serve_sync_dirty_limit int
var serve_sync_dirty_min_changed int
var serve_sync_dirty_block_period time.Duration

var src_exposed []storage.ExposedStorage
var src_storage []*storageInfo

var serveProgress *mpb.Progress
var serveBars []*mpb.Bar

func init() {
	rootCmd.AddCommand(cmdServe)
	cmdServe.Flags().StringVarP(&serve_addr, "addr", "a", ":5170", "Address to serve from")
	cmdServe.Flags().StringVarP(&serve_conf, "conf", "c", "silo.conf", "Configuration file")
	cmdServe.Flags().BoolVarP(&serve_progress, "progress", "p", false, "Show progress")
	cmdServe.Flags().BoolVarP(&serve_continuous, "continuous", "C", false, "Continuous sync")
	cmdServe.Flags().BoolVarP(&serve_any_order, "order", "o", false, "Any order (faster)")
	cmdServe.Flags().BoolVarP(&serve_sync_s3, "sync", "s", false, "Continuous sync to S3")

	cmdServe.Flags().StringVar(&serve_sync_endpoint, "s3endpoint", "", "Sync S3 endpoint")
	cmdServe.Flags().StringVar(&serve_sync_access, "s3access", "", "Sync S3 access token")
	cmdServe.Flags().StringVar(&serve_sync_secret, "s3secret", "", "Sync S3 secret token")
	cmdServe.Flags().StringVar(&serve_sync_bucket, "s3bucket", "", "Sync S3 bucket")

	cmdServe.Flags().DurationVar(&serve_sync_dirty_block_period, "sync-period", 1*time.Second, "Sync dirty block period")
	cmdServe.Flags().IntVar(&serve_sync_dirty_block_shift, "sync-shift", 10, "Sync dirty block shift")
	cmdServe.Flags().DurationVar(&serve_sync_block_max_age, "sync-maxage", 1*time.Second, "Sync dirty block maximum age")
	cmdServe.Flags().IntVar(&serve_sync_dirty_limit, "sync-limit", 64, "Sync dirty block limit")
	cmdServe.Flags().IntVar(&serve_sync_dirty_min_changed, "sync-min-changed", 16, "Sync dirty block min changed")
}

type storageInfo struct {
	tracker    *dirtytracker.DirtyTrackerRemote
	lockable   storage.LockableStorageProvider
	orderer    *blocks.PriorityBlockOrder
	num_blocks int
	block_size int
	name       string
	schema     string

	s3_sync_cancel func()
}

func runServe(ccmd *cobra.Command, args []string) {
	if serve_progress {
		serveProgress = mpb.New(
			mpb.WithOutput(color.Output),
			mpb.WithAutoRefresh(),
		)
		serveBars = make([]*mpb.Bar, 0)
	}

	src_exposed = make([]storage.ExposedStorage, 0)
	src_storage = make([]*storageInfo, 0)
	fmt.Printf("Starting silo serve %s\n", serve_addr)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		shutdown_everything()
		os.Exit(1)
	}()

	siloConf, err := config.ReadSchema(serve_conf)
	if err != nil {
		panic(err)
	}

	for i, s := range siloConf.Device {
		fmt.Printf("Setup storage %d [%s] size %s - %d\n", i, s.Name, s.Size, s.ByteSize())
		sinfo, err := setupStorageDevice(s)
		if err != nil {
			panic(fmt.Sprintf("Could not setup storage. %v", err))
		}

		src_storage = append(src_storage, sinfo)
	}

	// Setup listener here. When client connects, migrate data to it.

	l, err := net.Listen("tcp", serve_addr)
	if err != nil {
		shutdown_everything()
		panic("Listener issue...")
	}

	// Wait for a connection, and do a migration...
	fmt.Printf("Waiting for connection...\n")
	con, err := l.Accept()
	if err == nil {
		fmt.Printf("Received connection from %s\n", con.RemoteAddr().String())
		// Now we can migrate to the client...

		// Wrap the connection in a protocol
		pro := protocol.NewProtocolRW(context.TODO(), []io.Reader{con}, []io.Writer{con}, nil)
		go func() {
			_ = pro.Handle()
		}()

		// Lets go through each of the things we want to migrate...
		ctime := time.Now()

		var wg sync.WaitGroup

		for i, s := range src_storage {
			wg.Add(1)
			go func(index int, src *storageInfo) {
				err := migrateDevice(uint32(index), src.name, pro, src)
				if err != nil {
					fmt.Printf("There was an issue migrating the storage %d %v\n", index, err)
				}
				wg.Done()
			}(i, s)
		}
		wg.Wait()

		if serveProgress != nil {
			serveProgress.Wait()
		}
		fmt.Printf("\n\nMigration completed in %dms\n", time.Since(ctime).Milliseconds())

		con.Close()
	}
	shutdown_everything()
}

func shutdown_everything() {
	// first unlock everything
	fmt.Printf("Unlocking devices...\n")
	for _, i := range src_storage {
		i.lockable.Unlock()
		i.tracker.Close()
	}

	if serve_sync_s3 {
		fmt.Printf("Stopping S3 sync...\n")
		for _, i := range src_storage {
			i.s3_sync_cancel()
		}
	}

	fmt.Printf("Shutting down devices cleanly...\n")
	for _, p := range src_exposed {
		device := p.Device()

		fmt.Printf("Shutdown nbd device %s\n", device)
		_ = p.Shutdown()
	}
}

func setupStorageDevice(conf *config.DeviceSchema) (*storageInfo, error) {
	block_size := 1024 * 128

	num_blocks := (int(conf.ByteSize()) + block_size - 1) / block_size

	source, ex, err := device.NewDevice(conf)
	if err != nil {
		return nil, err
	}
	if ex != nil {
		fmt.Printf("Device %s exposed as %s\n", conf.Name, ex.Device())
		src_exposed = append(src_exposed, ex)
	}
	sourceMetrics := modules.NewMetrics(source)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceMetrics, block_size)

	var sourceDirty storage.StorageProvider
	sourceDirty = sourceDirtyLocal

	var s3_cancel func()
	var s3_ctx context.Context
	if serve_sync_s3 {
		s3_block_size := block_size >> serve_sync_dirty_block_shift
		s3SourceDirtyLocal, s3SourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceDirtyLocal, s3_block_size)
		s3Lockable := modules.NewLockable(s3SourceDirtyLocal)
		sourceDirty = s3Lockable

		s3Orderer := blocks.NewAnyBlockOrder(num_blocks, nil)

		dest, err := sources.NewS3StorageCreate(serve_sync_endpoint,
			serve_sync_access,
			serve_sync_secret,
			serve_sync_bucket,
			conf.Name,
			source.Size(),
			block_size)
		if err != nil {
			return nil, err
		}

		// Start syncing to S3 if it was requested of us...
		s3_ctx, s3_cancel = context.WithCancel(context.TODO())
		go func() {
			syncer := migrator.NewSyncer(s3_ctx, &migrator.Sync_config{
				Name:               conf.Name,
				Integrity:          false,
				Cancel_writes:      true,
				Dedupe_writes:      true,
				Tracker:            s3SourceDirtyRemote,
				Lockable:           s3Lockable,
				Destination:        dest,
				Orderer:            s3Orderer,
				Dirty_check_period: serve_sync_dirty_block_period,
				Dirty_block_getter: func() []uint {
					return s3SourceDirtyRemote.GetDirtyBlocks(
						serve_sync_block_max_age,
						serve_sync_dirty_limit,
						serve_sync_dirty_block_shift,
						serve_sync_dirty_min_changed)
				},
				Block_size: block_size,
				Progress_handler: func(p *migrator.MigrationProgress) {
					ood := s3SourceDirtyRemote.MeasureDirty()
					ood_age := s3SourceDirtyRemote.MeasureDirtyAge()
					fmt.Printf("S3 DIRTY STATUS %dms old, with %d blocks\n", time.Since(ood_age).Milliseconds(), ood)
				},
				Error_handler: func(b *storage.BlockInfo, err error) {
					fmt.Printf("There was an error syncing to S3.")
					panic(err)
				},
			})

			status, err := syncer.Sync(false, true)
			fmt.Printf("S3 Migration status %v err %v\n", status, err)
		}()
	}

	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirty, block_size, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	if ex != nil {
		ex.SetProvider(sourceStorage)
	}

	// Start monitoring blocks.

	var primary_orderer storage.BlockOrder
	primary_orderer = sourceMonitor

	if serve_any_order {
		primary_orderer = blocks.NewAnyBlockOrder(num_blocks, nil)
	}
	orderer := blocks.NewPriorityBlockOrder(num_blocks, primary_orderer)
	orderer.AddAll()

	schema := string(conf.Encode())

	sinfo := &storageInfo{
		tracker:        sourceDirtyRemote,
		lockable:       sourceStorage,
		orderer:        orderer,
		block_size:     block_size,
		num_blocks:     num_blocks,
		name:           conf.Name,
		schema:         schema,
		s3_sync_cancel: s3_cancel,
	}

	return sinfo, nil
}

// Migrate a device
func migrateDevice(dev_id uint32, name string,
	pro protocol.Protocol,
	sinfo *storageInfo) error {
	size := sinfo.lockable.Size()
	dest := protocol.NewToProtocol(uint64(size), dev_id, pro)

	err := dest.SendDevInfo(name, uint32(sinfo.block_size), sinfo.schema)
	if err != nil {
		return err
	}

	statusString := " "

	statusFn := func(s decor.Statistics) string {
		return statusString
	}

	var bar *mpb.Bar
	if serve_progress {
		bar = serveProgress.AddBar(int64(size),
			mpb.PrependDecorators(
				decor.Name(name, decor.WCSyncSpaceR),
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

		serveBars = append(serveBars, bar)
	}

	go func() {
		_ = dest.HandleNeedAt(func(offset int64, length int32) {
			// Prioritize blocks...
			end := uint64(offset + int64(length))
			if end > uint64(size) {
				end = uint64(size)
			}

			b_start := int(offset / int64(sinfo.block_size))
			b_end := int((end-1)/uint64(sinfo.block_size)) + 1
			for b := b_start; b < b_end; b++ {
				// Ask the orderer to prioritize these blocks...
				sinfo.orderer.PrioritiseBlock(b)
			}
		})
	}()

	go func() {
		_ = dest.HandleDontNeedAt(func(offset int64, length int32) {
			end := uint64(offset + int64(length))
			if end > uint64(size) {
				end = uint64(size)
			}

			b_start := int(offset / int64(sinfo.block_size))
			b_end := int((end-1)/uint64(sinfo.block_size)) + 1
			for b := b_start; b < b_end; b++ {
				sinfo.orderer.Remove(b)
			}
		})
	}()

	// FIXME: Sync integration here...
	sync_config := &migrator.Sync_config{
		Name:       "serve",
		Tracker:    sinfo.tracker,
		Lockable:   sinfo.lockable,
		Block_size: sinfo.block_size,
		Locker_handler: func() {
			_ = dest.SendEvent(&packets.Event{Type: packets.EventPreLock})
			sinfo.lockable.Lock()
			_ = dest.SendEvent(&packets.Event{Type: packets.EventPostLock})
		},
		Unlocker_handler: func() {
			_ = dest.SendEvent(&packets.Event{Type: packets.EventPreUnlock})
			sinfo.lockable.Unlock()
			_ = dest.SendEvent(&packets.Event{Type: packets.EventPostUnlock})
		},
		Error_handler: func(b *storage.BlockInfo, err error) {
			// For now...
			panic(err)
		},
		Destination: dest,
		Orderer:     sinfo.orderer,
	}

	last_value := uint64(0)
	last_time := time.Now()

	if serve_progress {

		sync_config.Progress_handler = func(p *migrator.MigrationProgress) {
			v := uint64(p.Ready_blocks) * uint64(sinfo.block_size)
			if v > size {
				v = size
			}
			bar.SetCurrent(int64(v))
			bar.EwmaIncrInt64(int64(v-last_value), time.Since(last_time))
			last_time = time.Now()
			last_value = v
		}
	} else {
		sync_config.Progress_handler = func(p *migrator.MigrationProgress) {
			fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
				name, p.Migrated_blocks, p.Total_blocks, p.Migrated_blocks_perc,
				p.Ready_blocks, p.Total_blocks, p.Ready_blocks_perc,
				p.Active_blocks)
		}
		sync_config.Error_handler = func(b *storage.BlockInfo, err error) {
			fmt.Printf("[%s] Error for block %d error %v\n", name, b.Block, err)
		}
	}

	sync_config.Hashes_handler = func(hashes map[uint][32]byte) {
		err = dest.SendHashes(hashes)
		if err != nil {
			panic(err) // FIXME
		}
	}

	sync_config.Dirty_check_period = 100 * time.Millisecond
	sync_config.Dirty_block_getter = func() []uint {
		blocks := sinfo.tracker.Sync()
		d := blocks.Collect(0, blocks.Length())

		if d != nil {
			err := dest.DirtyList(sinfo.block_size, d)
			if err != nil {
				panic(err) // FIXME
			}
		}
		return d
	}

	sync_config.Concurrency = map[int]int{
		storage.BlockTypeAny: 1000000,
	}

	sync_config.Integrity = true

	syncer := migrator.NewSyncer(context.TODO(), sync_config)
	_, err = syncer.Sync(true, serve_continuous)
	if err != nil {
		panic(err)
	}

	err = dest.SendEvent(&packets.Event{Type: packets.EventCompleted})
	if err != nil {
		return err
	}
	/*
		// Completed.
		if serve_progress {
			//		bar.SetCurrent(int64(size))
			//		bar.EwmaIncrInt64(int64(size-last_value), time.Since(last_time))
		}
	*/
	return nil
}
