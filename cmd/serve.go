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
}

type storageInfo struct {
	tracker    storage.TrackingStorageProvider
	lockable   storage.LockableStorageProvider
	orderer    *blocks.PriorityBlockOrder
	num_blocks int
	block_size int
	name       string
}

type storageConfig struct {
	Name   string
	Size   int
	Expose bool
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

	c := make(chan os.Signal)
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
		go pro.Handle()

		// Lets go through each of the things we want to migrate...
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

		con.Close()
	}

	shutdown_everything()
}

func shutdown_everything() {
	// first unlock everything
	fmt.Printf("Unlocking devices...\n")
	for _, i := range src_storage {
		i.lockable.Unlock()
	}

	fmt.Printf("Shutting down devices cleanly...\n")
	for _, p := range src_exposed {
		device := p.Device()

		fmt.Printf("Shutdown nbd device %s\n", device)
		p.Shutdown()
	}
}

func setupStorageDevice(conf *config.DeviceSchema) (*storageInfo, error) {
	block_size := 1024 * 64
	num_blocks := (conf.ByteSize() + block_size - 1) / block_size

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
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, block_size, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	ex.SetProvider(sourceStorage)

	// Start monitoring blocks.
	orderer := blocks.NewPriorityBlockOrder(num_blocks, sourceMonitor)
	orderer.AddAll()

	return &storageInfo{
		tracker:    sourceDirtyRemote,
		lockable:   sourceStorage,
		orderer:    orderer,
		block_size: block_size,
		num_blocks: num_blocks,
		name:       conf.Name,
	}, nil
}

// Migrate a device
func migrateDevice(dev_id uint32, name string,
	pro protocol.Protocol,
	sinfo *storageInfo) error {
	size := sinfo.lockable.Size()
	dest := protocol.NewToProtocol(uint64(size), dev_id, pro)

	dest.SendDevInfo(name, uint32(sinfo.block_size))

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

	go dest.HandleNeedAt(func(offset int64, length int32) {
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

	go dest.HandleDontNeedAt(func(offset int64, length int32) {
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

	conf := migrator.NewMigratorConfig().WithBlockSize(sinfo.block_size)
	conf.LockerHandler = func() {
		dest.SendEvent(protocol.EventPreLock)
		sinfo.lockable.Lock()
		dest.SendEvent(protocol.EventPostLock)
	}
	conf.UnlockerHandler = func() {
		dest.SendEvent(protocol.EventPreUnlock)
		sinfo.lockable.Unlock()
		dest.SendEvent(protocol.EventPostUnlock)
	}

	if serve_progress {
		last_value := uint64(0)
		last_time := time.Now()

		conf.ProgressHandler = func(p *migrator.MigrationProgress) {
			/*
				fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
					name, p.MigratedBlocks, p.TotalBlocks, p.MigratedBlocksPerc,
					p.ReadyBlocks, p.TotalBlocks, p.ReadyBlocksPerc,
					p.ActiveBlocks)
			*/
			v := uint64(p.ReadyBlocks) * size / uint64(p.TotalBlocks)
			bar.SetCurrent(int64(v))
			bar.EwmaIncrInt64(int64(v-last_value), time.Since(last_time))
			last_time = time.Now()
			last_value = v
		}
	}

	mig, err := migrator.NewMigrator(sinfo.tracker, dest, sinfo.orderer, conf)

	if err != nil {
		return err
	}

	// Now do the migration...
	err = mig.Migrate(sinfo.num_blocks)
	if err != nil {
		return err
	}

	// Wait for completion.
	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	// Optional: Enter a loop looking for more dirty blocks to migrate...

	for {
		blocks := mig.GetLatestDirty() //
		if !serve_continuous && blocks == nil {
			break
		}

		if blocks != nil {
			// Optional: Send the list of dirty blocks over...
			dest.DirtyList(blocks)

			//		fmt.Printf("[%s] Migrating dirty blocks %d\n", name, len(blocks))
			err := mig.MigrateDirty(blocks)
			if err != nil {
				return err
			}
		} else {
			mig.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}

	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	//	fmt.Printf("[%s] Migration completed\n", name)
	err = dest.SendEvent(protocol.EventCompleted)
	if err != nil {
		return err
	}

	// Completed.
	if serve_progress {
		bar.SetCurrent(int64(size))
	}

	return nil
}
