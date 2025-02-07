package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/spf13/cobra"
)

var (
	cmdSync = &cobra.Command{
		Use:   "sync",
		Short: "Continuous sync to s3",
		Long:  ``,
		Run:   runSync,
	}
)

// Configuration options
var syncConf string
var syncEndpoint string
var syncAccess string
var syncSecret string
var syncBucket string
var syncBlockSize int
var syncTimeLimit time.Duration
var syncReplay bool
var syncDirtyBlockShift int
var syncBlockMaxAge time.Duration
var syncDirtyMinChanged int
var syncDirtyPeriod time.Duration
var syncDirtyLimit int
var syncDummy bool

// Keep track of these for tidy up
var syncExposed []storage.ExposedStorage
var syncStorage []*syncStorageInfo

type syncStorageInfo struct {
	tracker     *dirtytracker.Remote
	lockable    storage.LockableProvider
	orderer     *blocks.PriorityBlockOrder
	numBlocks   int
	blockSize   int
	name        string
	destMetrics *modules.Metrics
	replayLog   string
}

func init() {
	rootCmd.AddCommand(cmdSync)
	cmdSync.Flags().StringVarP(&syncConf, "conf", "c", "silo.conf", "Configuration file")
	cmdSync.Flags().StringVarP(&syncEndpoint, "endpoint", "e", "", "S3 endpoint")
	cmdSync.Flags().StringVarP(&syncAccess, "access", "a", "", "S3 access")
	cmdSync.Flags().StringVarP(&syncSecret, "secret", "s", "", "S3 secret")
	cmdSync.Flags().StringVarP(&syncBucket, "bucket", "b", "", "S3 bucket")
	cmdSync.Flags().IntVarP(&syncBlockSize, "blocksize", "l", 1*1024*1024, "S3 block size")
	cmdSync.Flags().DurationVarP(&syncTimeLimit, "timelimit", "t", 30*time.Second, "Sync time limit")
	cmdSync.Flags().BoolVarP(&syncReplay, "replay", "r", false, "Replay existing binlog(s)")
	cmdSync.Flags().IntVarP(&syncDirtyBlockShift, "dirtyshift", "d", 10, "Dirty tracker block shift")
	cmdSync.Flags().DurationVarP(&syncBlockMaxAge, "dirtymaxage", "", 1*time.Second, "Dirty block max age")
	cmdSync.Flags().IntVarP(&syncDirtyMinChanged, "dirtyminchanged", "", 4, "Dirty block min subblock changes")
	cmdSync.Flags().DurationVarP(&syncDirtyPeriod, "dirtyperiod", "", 100*time.Millisecond, "Dirty block check period")
	cmdSync.Flags().IntVarP(&syncDirtyLimit, "dirtylimit", "", 16, "Dirty block limit per period")
	cmdSync.Flags().BoolVarP(&syncDummy, "dummy", "y", false, "Dummy destination")
}

/**
 * Run sync command
 *
 */
func runSync(_ *cobra.Command, _ []string) {
	syncExposed = make([]storage.ExposedStorage, 0)
	syncStorage = make([]*syncStorageInfo, 0)
	fmt.Printf("Starting silo s3 sync\n")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		syncShutdownEverything()
		os.Exit(1)
	}()

	// Load the configuration
	siloConf, err := config.ReadSchema(syncConf)
	if err != nil {
		panic(err)
	}

	// Go through and setup each device in turn
	for i, s := range siloConf.Device {
		fmt.Printf("Setup storage %d [%s] size %s - %d\n", i, s.Name, s.Size, s.ByteSize())
		sinfo, err := syncSetupDevice(s)
		if err != nil {
			panic(fmt.Sprintf("Could not setup storage. %v", err))
		}
		syncStorage = append(syncStorage, sinfo)
	}

	// Now lets go through each of the things we want to migrate/sync
	var wg sync.WaitGroup

	for i, s := range syncStorage {
		wg.Add(1)
		go func(index int, src *syncStorageInfo) {
			err := syncMigrateS3(uint32(index), src.name, src)
			if err != nil {
				fmt.Printf("There was an issue migrating the storage %d %v\n", index, err)
			}
			wg.Done()
		}(i, s)
	}
	wg.Wait()

	syncShutdownEverything()
}

/**
 * Setup a storage device for sync command
 *
 */
func syncSetupDevice(conf *config.DeviceSchema) (*syncStorageInfo, error) {
	blockSize := syncBlockSize // 1024 * 128

	numBlocks := (int(conf.ByteSize()) + blockSize - 1) / blockSize

	replayLog := ""

	// Get this from the conf if we are operating in replay mode.
	if syncReplay {
		replayLog = conf.Binlog
		conf.Binlog = ""
	}

	source, ex, err := device.NewDevice(conf)
	if err != nil {
		return nil, err
	}
	if ex != nil {
		fmt.Printf("Device %s exposed as %s\n", conf.Name, ex.Device())
		syncExposed = append(syncExposed, ex)
	}
	sourceMetrics := modules.NewMetrics(source)

	dirtyBlockSize := blockSize >> syncDirtyBlockShift

	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceMetrics, dirtyBlockSize)
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, blockSize, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	// Make sure any exposition is wired to go to the right place through the chain.
	if ex != nil {
		ex.SetProvider(sourceStorage)
	}

	// Setup a block order
	orderer := blocks.NewPriorityBlockOrder(numBlocks, sourceMonitor)
	orderer.AddAll()

	// Create a destination to migrate to
	var dest storage.Provider
	if syncDummy {
		dest = modules.NewNothing(sourceStorage.Size())
	} else {
		dest, err = sources.NewS3StorageCreate(false, syncEndpoint,
			syncAccess,
			syncSecret,
			syncBucket,
			conf.Name,
			sourceStorage.Size(),
			syncBlockSize)
		if err != nil {
			return nil, err
		}
	}

	// Return everything we need
	return &syncStorageInfo{
		tracker:     sourceDirtyRemote,
		lockable:    sourceStorage,
		orderer:     orderer,
		blockSize:   blockSize,
		numBlocks:   numBlocks,
		name:        conf.Name,
		destMetrics: modules.NewMetrics(dest),
		replayLog:   replayLog,
	}, nil
}

/**
 * Shutdown a device
 *
 */
func syncShutdownEverything() {
	// first unlock everything
	fmt.Printf("Unlocking and closing devices...\n")
	for _, i := range syncStorage {
		i.lockable.Unlock()
		i.tracker.Close()
		// Show some stats
		i.destMetrics.ShowStats(i.name)
	}

	fmt.Printf("Shutting down exposed devices cleanly...\n")
	for _, p := range syncExposed {
		device := p.Device()
		fmt.Printf("Shutdown nbd device %s\n", device)
		_ = p.Shutdown()
	}
}

/**
 * Migrate a device to S3
 *
 */
func syncMigrateS3(_ uint32, name string,
	sinfo *syncStorageInfo) error {

	destMetrics := modules.NewMetrics(sinfo.destMetrics)

	conf := migrator.NewConfig().WithBlockSize(syncBlockSize)
	conf.LockerHandler = func() {
		sinfo.lockable.Lock()
	}
	conf.UnlockerHandler = func() {
		sinfo.lockable.Unlock()
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 16,
	}
	conf.Integrity = false
	conf.CancelWrites = true
	conf.DedupeWrites = true

	conf.ProgressHandler = func(p *migrator.MigrationProgress) {
		fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d Total Mig: %d Canceled: %d Dupes: %d\n",
			name, p.MigratedBlocks, p.TotalBlocks, p.MigratedBlocksPerc,
			p.ReadyBlocks, p.TotalBlocks, p.ReadyBlocksPerc,
			p.ActiveBlocks, p.TotalMigratedBlocks, p.TotalCanceledBlocks, p.TotalDuplicatedBlocks)
		destMetrics.ShowStats("S3")
	}
	conf.ErrorHandler = func(b *storage.BlockInfo, err error) {
		fmt.Printf("[%s] Error for block %d error %v\n", name, b.Block, err)
	}

	mig, err := migrator.NewMigrator(sinfo.tracker, destMetrics, sinfo.orderer, conf)

	if err != nil {
		return err
	}

	ctx, cancelFn := context.WithCancel(context.TODO())

	// If we are replaying a log for this device, do it here
	if sinfo.replayLog != "" {
		fmt.Printf("Replay from binlog %s\n", sinfo.replayLog)
		// Open up a binlog, and replay it
		blr, err := modules.NewBinLogReplay(sinfo.replayLog, sinfo.lockable)
		if err != nil {
			cancelFn()
			return err
		}

		// Replay the binlog
		go func() {
			for {
				select {
				case <-ctx.Done():
					break
				default:
				}
				provErr, err := blr.Next(1, true)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					cancelFn()
					panic(err)
				} else if provErr != nil {
					cancelFn()
					panic(err)
				}
			}
		}()
	}

	numBlocks := (sinfo.tracker.Size() + uint64(syncBlockSize) - 1) / uint64(syncBlockSize)

	isNew := true

	if isNew {
		// Since it's a new source, it's all zeros. We don't need to do an initial migration.
		for b := 0; b < int(numBlocks); b++ {
			mig.SetMigratedBlock(b)
		}

		sinfo.tracker.TrackAt(0, int64(sinfo.tracker.Size()))
	} else {
		fmt.Printf("Doing migration...\n")

		// Now do the initial migration...
		err = mig.Migrate(int(numBlocks))
		if err != nil {
			cancelFn()
			return err
		}

		fmt.Printf("Waiting...\n")

		// Wait for completion.
		err = mig.WaitForCompletion()
		if err != nil {
			cancelFn()
			return err
		}
	}

	// Now enter a loop looking for more dirty blocks to migrate...

	fmt.Printf("Dirty loop...\n")

	getter := func() []uint {
		return sinfo.tracker.GetDirtyBlocks(syncBlockMaxAge, syncDirtyLimit, syncDirtyBlockShift, syncDirtyMinChanged)
	}

	ctime := time.Now()

	for {
		if time.Since(ctime) > syncTimeLimit {
			break
		}

		blocks := mig.GetLatestDirtyFunc(getter)

		if blocks != nil {
			err = mig.MigrateDirty(blocks)
			if err != nil {
				cancelFn()
				return err
			}
		} else {
			mig.Unlock()
		}
		time.Sleep(syncDirtyPeriod)
	}

	cancelFn() // Stop the write loop

	ood := sinfo.tracker.MeasureDirty()
	oodAge := sinfo.tracker.MeasureDirtyAge()
	fmt.Printf("DIRTY STATUS %dms old, with %d blocks\n", time.Since(oodAge).Milliseconds(), ood)

	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	return nil
}
