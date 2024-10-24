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
var sync_conf string
var sync_endpoint string
var sync_access string
var sync_secret string
var sync_bucket string
var sync_block_size int
var sync_time_limit time.Duration
var sync_replay bool
var sync_dirty_block_shift int
var sync_block_max_age time.Duration
var sync_dirty_min_changed int
var sync_dirty_period time.Duration
var sync_dirty_limit int
var sync_dummy bool

// Keep track of these for tidy up
var sync_exposed []storage.ExposedStorage
var sync_storage []*syncStorageInfo

type syncStorageInfo struct {
	tracker      *dirtytracker.DirtyTrackerRemote
	lockable     storage.LockableStorageProvider
	orderer      *blocks.PriorityBlockOrder
	num_blocks   int
	block_size   int
	name         string
	dest_metrics *modules.Metrics
	replay_log   string
}

func init() {
	rootCmd.AddCommand(cmdSync)
	cmdSync.Flags().StringVarP(&sync_conf, "conf", "c", "silo.conf", "Configuration file")
	cmdSync.Flags().StringVarP(&sync_endpoint, "endpoint", "e", "", "S3 endpoint")
	cmdSync.Flags().StringVarP(&sync_access, "access", "a", "", "S3 access")
	cmdSync.Flags().StringVarP(&sync_secret, "secret", "s", "", "S3 secret")
	cmdSync.Flags().StringVarP(&sync_bucket, "bucket", "b", "", "S3 bucket")
	cmdSync.Flags().IntVarP(&sync_block_size, "blocksize", "l", 1*1024*1024, "S3 block size")
	cmdSync.Flags().DurationVarP(&sync_time_limit, "timelimit", "t", 30*time.Second, "Sync time limit")
	cmdSync.Flags().BoolVarP(&sync_replay, "replay", "r", false, "Replay existing binlog(s)")
	cmdSync.Flags().IntVarP(&sync_dirty_block_shift, "dirtyshift", "d", 10, "Dirty tracker block shift")
	cmdSync.Flags().DurationVarP(&sync_block_max_age, "dirtymaxage", "", 1*time.Second, "Dirty block max age")
	cmdSync.Flags().IntVarP(&sync_dirty_min_changed, "dirtyminchanged", "", 4, "Dirty block min subblock changes")
	cmdSync.Flags().DurationVarP(&sync_dirty_period, "dirtyperiod", "", 100*time.Millisecond, "Dirty block check period")
	cmdSync.Flags().IntVarP(&sync_dirty_limit, "dirtylimit", "", 16, "Dirty block limit per period")
	cmdSync.Flags().BoolVarP(&sync_dummy, "dummy", "y", false, "Dummy destination")
}

/**
 * Run sync command
 *
 */
func runSync(ccmd *cobra.Command, args []string) {
	sync_exposed = make([]storage.ExposedStorage, 0)
	sync_storage = make([]*syncStorageInfo, 0)
	fmt.Printf("Starting silo s3 sync\n")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		sync_shutdown_everything()
		os.Exit(1)
	}()

	// Load the configuration
	siloConf, err := config.ReadSchema(sync_conf)
	if err != nil {
		panic(err)
	}

	// Go through and setup each device in turn
	for i, s := range siloConf.Device {
		fmt.Printf("Setup storage %d [%s] size %s - %d\n", i, s.Name, s.Size, s.ByteSize())
		sinfo, err := sync_setup_device(s)
		if err != nil {
			panic(fmt.Sprintf("Could not setup storage. %v", err))
		}
		sync_storage = append(sync_storage, sinfo)
	}

	// Now lets go through each of the things we want to migrate/sync
	var wg sync.WaitGroup

	for i, s := range sync_storage {
		wg.Add(1)
		go func(index int, src *syncStorageInfo) {
			err := sync_migrate_s3(uint32(index), src.name, src)
			if err != nil {
				fmt.Printf("There was an issue migrating the storage %d %v\n", index, err)
			}
			wg.Done()
		}(i, s)
	}
	wg.Wait()

	sync_shutdown_everything()
}

/**
 * Setup a storage device for sync command
 *
 */
func sync_setup_device(conf *config.DeviceSchema) (*syncStorageInfo, error) {
	block_size := sync_block_size // 1024 * 128

	num_blocks := (int(conf.ByteSize()) + block_size - 1) / block_size

	replay_log := ""

	// Get this from the conf if we are operating in replay mode.
	if sync_replay {
		replay_log = conf.Binlog
		conf.Binlog = ""
	}

	source, ex, err := device.NewDevice(conf)
	if err != nil {
		return nil, err
	}
	if ex != nil {
		fmt.Printf("Device %s exposed as %s\n", conf.Name, ex.Device())
		sync_exposed = append(sync_exposed, ex)
	}
	sourceMetrics := modules.NewMetrics(source)

	dirty_block_size := block_size >> sync_dirty_block_shift

	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceMetrics, dirty_block_size)
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, block_size, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	// Make sure any exposition is wired to go to the right place through the chain.
	if ex != nil {
		ex.SetProvider(sourceStorage)
	}

	// Setup a block order
	orderer := blocks.NewPriorityBlockOrder(num_blocks, sourceMonitor)
	orderer.AddAll()

	// Create a destination to migrate to
	var dest storage.StorageProvider
	if sync_dummy {
		dest = modules.NewNothing(sourceStorage.Size())
	} else {
		dest, err = sources.NewS3StorageCreate(false, sync_endpoint,
			sync_access,
			sync_secret,
			sync_bucket,
			conf.Name,
			sourceStorage.Size(),
			sync_block_size)
		if err != nil {
			return nil, err
		}
	}

	// Return everything we need
	return &syncStorageInfo{
		tracker:      sourceDirtyRemote,
		lockable:     sourceStorage,
		orderer:      orderer,
		block_size:   block_size,
		num_blocks:   num_blocks,
		name:         conf.Name,
		dest_metrics: modules.NewMetrics(dest),
		replay_log:   replay_log,
	}, nil
}

/**
 * Shutdown a device
 *
 */
func sync_shutdown_everything() {
	// first unlock everything
	fmt.Printf("Unlocking and closing devices...\n")
	for _, i := range sync_storage {
		i.lockable.Unlock()
		i.tracker.Close()
		// Show some stats
		i.dest_metrics.ShowStats(i.name)
	}

	fmt.Printf("Shutting down exposed devices cleanly...\n")
	for _, p := range sync_exposed {
		device := p.Device()
		fmt.Printf("Shutdown nbd device %s\n", device)
		_ = p.Shutdown()
	}
}

/**
 * Migrate a device to S3
 *
 */
func sync_migrate_s3(_ uint32, name string,
	sinfo *syncStorageInfo) error {

	dest_metrics := modules.NewMetrics(sinfo.dest_metrics)

	conf := migrator.NewMigratorConfig().WithBlockSize(sync_block_size)
	conf.Locker_handler = func() {
		sinfo.lockable.Lock()
	}
	conf.Unlocker_handler = func() {
		sinfo.lockable.Unlock()
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 16,
	}
	conf.Integrity = false
	conf.Cancel_writes = true
	conf.Dedupe_writes = true

	conf.Progress_handler = func(p *migrator.MigrationProgress) {
		fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d Total Mig: %d Canceled: %d Dupes: %d\n",
			name, p.Migrated_blocks, p.Total_blocks, p.Migrated_blocks_perc,
			p.Ready_blocks, p.Total_blocks, p.Ready_blocks_perc,
			p.Active_blocks, p.Total_Migrated_blocks, p.Total_Canceled_blocks, p.Total_Duplicated_blocks)
		dest_metrics.ShowStats("S3")
	}
	conf.Error_handler = func(b *storage.BlockInfo, err error) {
		fmt.Printf("[%s] Error for block %d error %v\n", name, b.Block, err)
	}

	// Show logging for S3 writes
	log_dest := modules.NewLogger(dest_metrics, "S3")

	mig, err := migrator.NewMigrator(sinfo.tracker, log_dest, sinfo.orderer, conf)

	if err != nil {
		return err
	}

	ctx, cancelFn := context.WithCancel(context.TODO())

	// If we are replaying a log for this device, do it here
	if sinfo.replay_log != "" {
		fmt.Printf("Replay from binlog %s\n", sinfo.replay_log)
		// Open up a binlog, and replay it
		blr, err := modules.NewBinLogReplay(sinfo.replay_log, sinfo.lockable)
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
				err := blr.ExecuteNext(1)
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					cancelFn()
					panic(err)
				}
			}
		}()
	}

	num_blocks := (sinfo.tracker.Size() + uint64(sync_block_size) - 1) / uint64(sync_block_size)

	is_new := true

	if is_new {
		// Since it's a new source, it's all zeros. We don't need to do an initial migration.
		for b := 0; b < int(num_blocks); b++ {
			mig.SetMigratedBlock(b)
		}

		sinfo.tracker.TrackAt(0, int64(sinfo.tracker.Size()))
	} else {
		fmt.Printf("Doing migration...\n")

		// Now do the initial migration...
		err = mig.Migrate(int(num_blocks))
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
		return sinfo.tracker.GetDirtyBlocks(sync_block_max_age, sync_dirty_limit, sync_dirty_block_shift, sync_dirty_min_changed)
	}

	ctime := time.Now()

	for {
		if time.Since(ctime) > sync_time_limit {
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
		time.Sleep(sync_dirty_period)
	}

	cancelFn() // Stop the write loop

	ood := sinfo.tracker.MeasureDirty()
	ood_age := sinfo.tracker.MeasureDirtyAge()
	fmt.Printf("DIRTY STATUS %dms old, with %d blocks\n", time.Since(ood_age).Milliseconds(), ood)

	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	return nil
}
