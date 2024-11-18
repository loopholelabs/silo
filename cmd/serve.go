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
	"github.com/loopholelabs/logging"
	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
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

var serveAddr string
var serveConf string
var serveProgress bool
var serveContinuous bool
var serveAnyOrder bool

var srcExposed []storage.ExposedStorage
var srcStorage []*storageInfo

var serveProgressBar *mpb.Progress
var serveBars []*mpb.Bar

var serveDebug bool

func init() {
	rootCmd.AddCommand(cmdServe)
	cmdServe.Flags().StringVarP(&serveAddr, "addr", "a", ":5170", "Address to serve from")
	cmdServe.Flags().StringVarP(&serveConf, "conf", "c", "silo.conf", "Configuration file")
	cmdServe.Flags().BoolVarP(&serveProgress, "progress", "p", false, "Show progress")
	cmdServe.Flags().BoolVarP(&serveContinuous, "continuous", "C", false, "Continuous sync")
	cmdServe.Flags().BoolVarP(&serveAnyOrder, "order", "o", false, "Any order (faster)")
	cmdServe.Flags().BoolVarP(&serveDebug, "debug", "d", false, "Debug logging (trace)")
}

type storageInfo struct {
	//	tracker       storage.TrackingStorageProvider
	tracker   *dirtytracker.Remote
	lockable  storage.LockableProvider
	orderer   *blocks.PriorityBlockOrder
	numBlocks int
	blockSize int
	name      string
	schema    string
}

func runServe(_ *cobra.Command, _ []string) {
	var log types.RootLogger
	if serveDebug {
		log = logging.New(logging.Zerolog, "silo.serve", os.Stderr)
		log.SetLevel(types.TraceLevel)
	}

	if serveProgress {
		serveProgressBar = mpb.New(
			mpb.WithOutput(color.Output),
			mpb.WithAutoRefresh(),
		)
		serveBars = make([]*mpb.Bar, 0)
	}

	srcExposed = make([]storage.ExposedStorage, 0)
	srcStorage = make([]*storageInfo, 0)
	fmt.Printf("Starting silo serve %s\n", serveAddr)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		shutdownEverything(log)
		os.Exit(1)
	}()

	siloConf, err := config.ReadSchema(serveConf)
	if err != nil {
		panic(err)
	}

	for i, s := range siloConf.Device {
		fmt.Printf("Setup storage %d [%s] size %s - %d\n", i, s.Name, s.Size, s.ByteSize())
		sinfo, err := setupStorageDevice(s, log)
		if err != nil {
			panic(fmt.Sprintf("Could not setup storage. %v", err))
		}

		srcStorage = append(srcStorage, sinfo)
	}

	// Setup listener here. When client connects, migrate data to it.

	l, err := net.Listen("tcp", serveAddr)
	if err != nil {
		shutdownEverything(log)
		panic("Listener issue...")
	}

	// Wait for a connection, and do a migration...
	fmt.Printf("Waiting for connection...\n")
	con, err := l.Accept()
	if err == nil {
		fmt.Printf("Received connection from %s\n", con.RemoteAddr().String())
		// Now we can migrate to the client...

		// Wrap the connection in a protocol
		pro := protocol.NewRW(context.TODO(), []io.Reader{con}, []io.Writer{con}, nil)
		go func() {
			_ = pro.Handle()
		}()

		// Lets go through each of the things we want to migrate...
		ctime := time.Now()

		var wg sync.WaitGroup

		for i, s := range srcStorage {
			wg.Add(1)
			go func(index int, src *storageInfo) {
				err := migrateDevice(log, uint32(index), src.name, pro, src)
				if err != nil {
					fmt.Printf("There was an issue migrating the storage %d %v\n", index, err)
				}
				wg.Done()
			}(i, s)
		}
		wg.Wait()

		if serveProgressBar != nil {
			serveProgressBar.Wait()
		}
		fmt.Printf("\n\nMigration completed in %dms\n", time.Since(ctime).Milliseconds())

		con.Close()
	}
	shutdownEverything(log)
}

func shutdownEverything(_ types.RootLogger) {
	// first unlock everything
	fmt.Printf("Unlocking devices...\n")
	for _, i := range srcStorage {
		i.lockable.Unlock()
		i.tracker.Close()
	}

	fmt.Printf("Shutting down devices cleanly...\n")
	for _, p := range srcExposed {
		device := p.Device()

		fmt.Printf("Shutdown nbd device %s\n", device)
		_ = p.Shutdown()
	}
}

func setupStorageDevice(conf *config.DeviceSchema, log types.RootLogger) (*storageInfo, error) {
	source, ex, err := device.NewDeviceWithLogging(conf, log)
	if err != nil {
		return nil, err
	}
	if ex != nil {
		fmt.Printf("Device %s exposed as %s\n", conf.Name, ex.Device())
		srcExposed = append(srcExposed, ex)
	}

	blockSize := 1024 * 128

	if conf.BlockSize != "" {
		blockSize = int(conf.ByteBlockSize())
	}

	numBlocks := (int(conf.ByteSize()) + blockSize - 1) / blockSize

	sourceMetrics := modules.NewMetrics(source)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceMetrics, blockSize)
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, blockSize, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	if ex != nil {
		ex.SetProvider(sourceStorage)
	}

	// Start monitoring blocks.

	var primaryOrderer storage.BlockOrder
	primaryOrderer = sourceMonitor

	if serveAnyOrder {
		primaryOrderer = blocks.NewAnyBlockOrder(numBlocks, nil)
	}
	orderer := blocks.NewPriorityBlockOrder(numBlocks, primaryOrderer)
	orderer.AddAll()

	schema := string(conf.Encode())

	sinfo := &storageInfo{
		tracker:   sourceDirtyRemote,
		lockable:  sourceStorage,
		orderer:   orderer,
		blockSize: blockSize,
		numBlocks: numBlocks,
		name:      conf.Name,
		schema:    schema,
	}

	return sinfo, nil
}

// Migrate a device
func migrateDevice(log types.RootLogger, devID uint32, name string,
	pro protocol.Protocol,
	sinfo *storageInfo) error {
	size := sinfo.lockable.Size()
	dest := protocol.NewToProtocol(size, devID, pro)

	err := dest.SendDevInfo(name, uint32(sinfo.blockSize), sinfo.schema)
	if err != nil {
		return err
	}

	statusString := " "

	statusFn := func(_ decor.Statistics) string {
		return statusString
	}

	var bar *mpb.Bar
	if serveProgress {
		bar = serveProgressBar.AddBar(int64(size),
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
			if end > size {
				end = size
			}

			bStart := int(offset / int64(sinfo.blockSize))
			bEnd := int((end-1)/uint64(sinfo.blockSize)) + 1
			for b := bStart; b < bEnd; b++ {
				// Ask the orderer to prioritize these blocks...
				sinfo.orderer.PrioritiseBlock(b)
			}
		})
	}()

	go func() {
		_ = dest.HandleDontNeedAt(func(offset int64, length int32) {
			end := uint64(offset + int64(length))
			if end > size {
				end = size
			}

			bStart := int(offset / int64(sinfo.blockSize))
			bEnd := int((end-1)/uint64(sinfo.blockSize)) + 1
			for b := bStart; b < bEnd; b++ {
				sinfo.orderer.Remove(b)
			}
		})
	}()

	conf := migrator.NewConfig().WithBlockSize(sinfo.blockSize)
	conf.Logger = log
	conf.LockerHandler = func() {
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPreLock})
		sinfo.lockable.Lock()
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPostLock})
	}
	conf.UnlockerHandler = func() {
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPreUnlock})
		sinfo.lockable.Unlock()
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPostUnlock})
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 1000000,
	}
	conf.ErrorHandler = func(_ *storage.BlockInfo, err error) {
		// For now...
		panic(err)
	}
	conf.Integrity = true

	lastValue := uint64(0)
	lastTime := time.Now()

	if serveProgress {

		conf.ProgressHandler = func(p *migrator.MigrationProgress) {
			v := uint64(p.ReadyBlocks) * uint64(sinfo.blockSize)
			if v > size {
				v = size
			}
			bar.SetCurrent(int64(v))
			bar.EwmaIncrInt64(int64(v-lastValue), time.Since(lastTime))
			lastTime = time.Now()
			lastValue = v
		}
	} else {
		conf.ProgressHandler = func(p *migrator.MigrationProgress) {
			fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
				name, p.MigratedBlocks, p.TotalBlocks, p.MigratedBlocksPerc,
				p.ReadyBlocks, p.TotalBlocks, p.ReadyBlocksPerc,
				p.ActiveBlocks)
		}
		conf.ErrorHandler = func(b *storage.BlockInfo, err error) {
			fmt.Printf("[%s] Error for block %d error %v\n", name, b.Block, err)
		}
	}

	mig, err := migrator.NewMigrator(sinfo.tracker, dest, sinfo.orderer, conf)

	if err != nil {
		return err
	}

	migrateBlocks := sinfo.numBlocks

	// Now do the migration...
	err = mig.Migrate(migrateBlocks)
	if err != nil {
		return err
	}

	// Wait for completion.
	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	hashes := mig.GetHashes() // Get the initial hashes and send them over for verification...
	err = dest.SendHashes(hashes)
	if err != nil {
		return err
	}

	// Optional: Enter a loop looking for more dirty blocks to migrate...
	for {
		blocks := mig.GetLatestDirty() //
		if !serveContinuous && blocks == nil {
			break
		}

		if blocks != nil {
			// Optional: Send the list of dirty blocks over...
			err := dest.DirtyList(conf.BlockSize, blocks)
			if err != nil {
				return err
			}

			//		fmt.Printf("[%s] Migrating dirty blocks %d\n", name, len(blocks))
			err = mig.MigrateDirty(blocks)
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
