package main

import (
	"fmt"
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

var sync_exposed []storage.ExposedStorage
var sync_storage []*storageInfo

func init() {
	rootCmd.AddCommand(cmdSync)
	cmdSync.Flags().StringVarP(&sync_conf, "conf", "c", "silo.conf", "Configuration file")
	cmdSync.Flags().StringVarP(&sync_endpoint, "endpoint", "e", "", "S3 endpoint")
	cmdSync.Flags().StringVarP(&sync_access, "access", "a", "", "S3 access")
	cmdSync.Flags().StringVarP(&sync_secret, "secret", "s", "", "S3 secret")
	cmdSync.Flags().StringVarP(&sync_bucket, "bucket", "b", "", "S3 bucket")
	cmdSync.Flags().IntVarP(&sync_block_size, "blocksize", "l", 4*1024*1024, "S3 block size")
}

func runSync(ccmd *cobra.Command, args []string) {

	sync_exposed = make([]storage.ExposedStorage, 0)
	sync_storage = make([]*storageInfo, 0)
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
		src_storage = append(src_storage, sinfo)
	}

	// Lets go through each of the things we want to migrate/sync

	var wg sync.WaitGroup

	for i, s := range src_storage {
		wg.Add(1)
		go func(index int, src *storageInfo) {
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

func setupSyncStorageDevice(conf *config.DeviceSchema) (*storageInfo, error) {
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
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceMetrics, block_size)
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, block_size, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	if ex != nil {
		ex.SetProvider(sourceStorage)
	}

	// Start monitoring blocks.

	go func() {
		ticker := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-ticker.C:
				fmt.Printf("Dirty %d blocks\n", sourceDirtyRemote.MeasureDirty())
			}
		}
	}()

	var primary_orderer storage.BlockOrder
	primary_orderer = sourceMonitor

	if serve_any_order {
		primary_orderer = blocks.NewAnyBlockOrder(num_blocks, nil)
	}
	orderer := blocks.NewPriorityBlockOrder(num_blocks, primary_orderer)
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

func sync_shutdown_everything() {
	// first unlock everything
	fmt.Printf("Unlocking devices...\n")
	for _, i := range sync_storage {
		i.lockable.Unlock()
		i.tracker.Close()
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
	sinfo *storageInfo) error {

	dest, err := sources.NewS3StorageCreate(sync_endpoint,
		sync_access,
		sync_secret,
		sync_bucket,
		sinfo.name,
		sinfo.lockable.Size(),
		sync_block_size)

	if err != nil {
		return err
	}

	conf := migrator.NewMigratorConfig().WithBlockSize(sync_block_size)
	conf.Locker_handler = func() {
		sinfo.lockable.Lock()
	}
	conf.Unlocker_handler = func() {
		sinfo.lockable.Unlock()
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 1000000,
	}
	conf.Integrity = false

	conf.Progress_handler = func(p *migrator.MigrationProgress) {
		fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
			name, p.Migrated_blocks, p.Total_blocks, p.Migrated_blocks_perc,
			p.Ready_blocks, p.Total_blocks, p.Ready_blocks_perc,
			p.Active_blocks)
	}
	conf.Error_handler = func(b *storage.BlockInfo, err error) {
		fmt.Printf("[%s] Error for block %d error %v\n", name, b.Block, err)
	}

	log_dest := modules.NewLogger(dest, "S3")

	mig, err := migrator.NewMigrator(sinfo.tracker, log_dest, sinfo.orderer, conf)

	if err != nil {
		return err
	}

	fmt.Printf("Doing migration...\n")

	// Now do the initial migration...
	num_blocks := (sinfo.tracker.Size() + uint64(sync_block_size) - 1) / uint64(sync_block_size)
	err = mig.Migrate(int(num_blocks))
	if err != nil {
		return err
	}

	fmt.Printf("Waiting...\n")

	// Wait for completion.
	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	// Enter a loop looking for more dirty blocks to migrate...

	fmt.Printf("Dirty loop...\n")

	for {
		blocks := mig.GetLatestDirty() //

		if blocks != nil {
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

	return nil
}
