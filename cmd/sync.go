package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand"
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

var sync_conf string
var sync_endpoint string
var sync_access string
var sync_secret string
var sync_bucket string
var sync_block_size int
var sync_time_limit time.Duration
var sync_random_writes bool
var sync_write_period time.Duration
var sync_dirty_block_shift int
var sync_dummy bool

var sync_exposed []storage.ExposedStorage
var sync_storage []*syncStorageInfo

var write_rand_source *rand.Rand

type syncStorageInfo struct {
	tracker     *dirtytracker.DirtyTrackerRemote
	lockable    storage.LockableStorageProvider
	orderer     *blocks.PriorityBlockOrder
	num_blocks  int
	block_size  int
	name        string
	destMetrics *modules.Metrics
}

func init() {
	rootCmd.AddCommand(cmdSync)
	cmdSync.Flags().StringVarP(&sync_conf, "conf", "c", "silo.conf", "Configuration file")
	cmdSync.Flags().StringVarP(&sync_endpoint, "endpoint", "e", "", "S3 endpoint")
	cmdSync.Flags().StringVarP(&sync_access, "access", "a", "", "S3 access")
	cmdSync.Flags().StringVarP(&sync_secret, "secret", "s", "", "S3 secret")
	cmdSync.Flags().StringVarP(&sync_bucket, "bucket", "b", "", "S3 bucket")
	cmdSync.Flags().IntVarP(&sync_block_size, "blocksize", "l", 4*1024*1024, "S3 block size")
	cmdSync.Flags().DurationVarP(&sync_time_limit, "timelimit", "t", 10*time.Second, "Sync time limit")
	cmdSync.Flags().BoolVarP(&sync_random_writes, "randomwrites", "r", false, "Perform random writes")
	cmdSync.Flags().DurationVarP(&sync_write_period, "writeperiod", "w", 100*time.Millisecond, "Random write period")
	cmdSync.Flags().IntVarP(&sync_dirty_block_shift, "dirtyshift", "d", 10, "Dirty tracker block shift")
	cmdSync.Flags().BoolVarP(&sync_dummy, "dummy", "y", false, "Dummy destination")
}

func runSync(ccmd *cobra.Command, args []string) {

	write_rand_source = rand.New(rand.NewSource(1))

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

	siloConf, err := config.ReadSchema(sync_conf)
	if err != nil {
		panic(err)
	}

	for i, s := range siloConf.Device {

		fmt.Printf("Setup storage %d [%s] size %s - %d\n", i, s.Name, s.Size, s.ByteSize())
		sinfo, err := setupSyncStorageDevice(s)
		if err != nil {
			panic(fmt.Sprintf("Could not setup storage. %v", err))
		}
		sync_storage = append(sync_storage, sinfo)
	}

	// Lets go through each of the things we want to migrate/sync

	var wg sync.WaitGroup

	for i, s := range sync_storage {
		wg.Add(1)
		go func(index int, src *syncStorageInfo) {
			err := migrateDeviceS3(uint32(index), src.name, src)
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
func setupSyncStorageDevice(conf *config.DeviceSchema) (*syncStorageInfo, error) {
	block_size := sync_block_size // 1024 * 128

	num_blocks := (int(conf.ByteSize()) + block_size - 1) / block_size

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

	var dest storage.StorageProvider

	if sync_dummy {
		dest = modules.NewNothing(sourceStorage.Size())
	} else {
		dest, err = sources.NewS3StorageCreate(sync_endpoint,
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

	return &syncStorageInfo{
		tracker:     sourceDirtyRemote,
		lockable:    sourceStorage,
		orderer:     orderer,
		block_size:  block_size,
		num_blocks:  num_blocks,
		name:        conf.Name,
		destMetrics: modules.NewMetrics(dest),
	}, nil
}

func sync_shutdown_everything() {
	// first unlock everything
	fmt.Printf("Unlocking devices...\n")
	for _, i := range sync_storage {
		i.lockable.Unlock()
		i.tracker.Close()
		// Show some stats
		i.destMetrics.ShowStats(i.name)
	}

	fmt.Printf("Shutting down devices cleanly...\n")
	for _, p := range sync_exposed {
		device := p.Device()

		fmt.Printf("Shutdown nbd device %s\n", device)
		_ = p.Shutdown()
	}
}

// Migrate a device
func migrateDeviceS3(dev_id uint32, name string,
	sinfo *syncStorageInfo) error {

	dest_metrics := modules.NewMetrics(sinfo.destMetrics)

	conf := migrator.NewMigratorConfig().WithBlockSize(sync_block_size)
	conf.Locker_handler = func() {
		sinfo.lockable.Lock()
	}
	conf.Unlocker_handler = func() {
		sinfo.lockable.Unlock()
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 8,
	}
	conf.Integrity = false

	conf.Progress_handler = func(p *migrator.MigrationProgress) {
		fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
			name, p.Migrated_blocks, p.Total_blocks, p.Migrated_blocks_perc,
			p.Ready_blocks, p.Total_blocks, p.Ready_blocks_perc,
			p.Active_blocks)
		dest_metrics.ShowStats("S3")
	}
	conf.Error_handler = func(b *storage.BlockInfo, err error) {
		fmt.Printf("[%s] Error for block %d error %v\n", name, b.Block, err)
	}

	log_dest := modules.NewLogger(dest_metrics, "S3")

	mig, err := migrator.NewMigrator(sinfo.tracker, log_dest, sinfo.orderer, conf)

	if err != nil {
		return err
	}

	ctx, cancelFn := context.WithCancel(context.TODO())

	// Do random writes to the device for testing
	//
	if sync_random_writes {
		go func() {
			dev_size := int(sinfo.tracker.Size())
			t := time.NewTicker(sync_write_period)
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					// Do a random write to the device...
					sizes := []int{4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024}
					size := sizes[write_rand_source.Intn(len(sizes))]
					offset := write_rand_source.Intn(dev_size)
					// Now align, and shift if needed
					if offset+size > dev_size {
						offset = dev_size - size
					}
					offset = 4096 * (offset / 4096)
					// Now do a write...
					buffer := make([]byte, size)
					crand.Read(buffer)
					fmt.Printf("-Write- %d %d\n", offset, len(buffer))
					_, err := sinfo.lockable.WriteAt(buffer, int64(offset))
					if err != nil {
						panic(err)
					}
				}
			}
		}()
	}

	num_blocks := (sinfo.tracker.Size() + uint64(sync_block_size) - 1) / uint64(sync_block_size)

	// NB: We only need to do this for existing sources.
	/*
		fmt.Printf("Doing migration...\n")

		// Now do the initial migration...
		err = mig.Migrate(int(num_blocks))
		if err != nil {
			return err
		}
	*/

	// Since it's a new source, it's all zeros. We don't need to do an initial migration.
	for b := 0; b < int(num_blocks); b++ {
		mig.SetMigratedBlock(b)
	}

	sinfo.tracker.TrackAt(0, int64(sinfo.tracker.Size()))

	fmt.Printf("Waiting...\n")

	// Wait for completion.
	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	// Enter a loop looking for more dirty blocks to migrate...

	fmt.Printf("Dirty loop...\n")

	// Dirty block selection params go here
	max_age := 5000 * time.Millisecond
	limit := 4
	min_changed := 8
	//

	getter := func() []uint {
		return sinfo.tracker.GetDirtyBlocks(max_age, limit, sync_dirty_block_shift, min_changed)
	}

	ctime := time.Now()

	block_histo := make(map[uint]int)

	for {
		if time.Since(ctime) > sync_time_limit {
			break
		}

		// Show dirty status...
		ood := sinfo.tracker.MeasureDirty()
		ood_age := sinfo.tracker.MeasureDirtyAge()
		fmt.Printf("DIRTY STATUS %dms old, with %d blocks\n", time.Since(ood_age).Milliseconds(), ood)

		blocks := mig.GetLatestDirtyFunc(getter) //

		if blocks != nil {
			fmt.Printf("Dirty blocks %v\n", blocks)

			// Update the histogram for blocks...
			for _, b := range blocks {
				_, ok := block_histo[b]
				if ok {
					block_histo[b]++
				} else {
					block_histo[b] = 1
				}
			}

			err = mig.MigrateDirty(blocks)
			if err != nil {
				return err
			}
		} else {
			mig.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancelFn() // Stop the write loop

	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	// Check the histogram...
	counts := make(map[int]int)
	for _, count := range block_histo {
		_, ok := counts[count]
		if ok {
			counts[count]++
		} else {
			counts[count] = 1
		}
	}

	for c, cc := range counts {
		fmt.Printf("Write histo %d - %d\n", c, cc)
	}

	ood := sinfo.tracker.MeasureDirty()
	ood_age := sinfo.tracker.MeasureDirtyAge()

	fmt.Printf("DIRTY STATUS %dms old, with %d blocks\n", time.Since(ood_age).Milliseconds(), ood)

	// Check how many dirty blocks were left, and how out of date we are...
	//left_blocks := sinfo.tracker.GetDirtyBlocks(0, 1000000, sync_dirty_block_shift, 0)

	return nil
}
