package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/spf13/cobra"
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
var serve_expose_dev bool
var serve_mount_dev bool

var src_exposed []storage.ExposedStorage
var src_storage []*storageInfo

func init() {
	rootCmd.AddCommand(cmdServe)
	cmdServe.Flags().StringVarP(&serve_addr, "addr", "a", ":5170", "Address to serve from")
	cmdServe.Flags().BoolVarP(&serve_expose_dev, "expose", "e", false, "Expose as nbd dev")
	cmdServe.Flags().BoolVarP(&serve_mount_dev, "mount", "m", false, "mkfs.ext4 and mount")
}

type storageInfo struct {
	tracker    storage.TrackingStorageProvider
	lockable   storage.LockableStorageProvider
	orderer    *blocks.PriorityBlockOrder
	num_blocks int
	block_size int
}

func runServe(ccmd *cobra.Command, args []string) {
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

	for _, s := range []int{1024 * 1024 * 1024, 100 * 1024 * 1024} {
		sinfo, err := setupStorageDevice(s)
		if err != nil {
			panic("Could not setup storage.")
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
		pro := protocol.NewProtocolRW(context.TODO(), con, con, nil)
		go pro.Handle()

		// Lets go through each of the things we want to migrate...
		var wg sync.WaitGroup

		for i, s := range src_storage {
			wg.Add(1)
			go func(index int, src *storageInfo) {
				err := migrateDevice(uint32(index), fmt.Sprintf("dev_%d", index), pro, src)
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

	if serve_expose_dev {
		fmt.Printf("Shutting down devices cleanly...\n")
		for _, e := range src_exposed {
			src_dev_shutdown(e)
		}
	}
}

func setupStorageDevice(size int) (*storageInfo, error) {
	block_size := 1024 * 64
	num_blocks := (size + block_size - 1) / block_size

	cr := func(s int) storage.StorageProvider {
		return sources.NewMemoryStorage(s)
	}
	// Setup some sharded memory storage (for concurrent write speed)
	source := modules.NewShardedStorage(size, size/1024, cr)
	sourceMetrics := modules.NewMetrics(source)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceMetrics, block_size)
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, block_size, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	// Start monitoring blocks.
	orderer := blocks.NewPriorityBlockOrder(num_blocks, sourceMonitor)
	orderer.AddAll()

	if serve_expose_dev {
		p, err := src_dev_setup(sourceStorage)
		if err != nil {
			return nil, err
		}
		src_exposed = append(src_exposed, p)
	}
	return &storageInfo{
		tracker:    sourceDirtyRemote,
		lockable:   sourceStorage,
		orderer:    orderer,
		block_size: block_size,
		num_blocks: num_blocks,
	}, nil
}

// Migrate a device
func migrateDevice(dev_id uint32, name string,
	pro protocol.Protocol,
	sinfo *storageInfo) error {
	size := sinfo.lockable.Size()
	dest := protocol.NewToProtocol(uint64(size), dev_id, pro)

	dest.SendDevInfo(name, uint32(sinfo.block_size))

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
	conf.ProgressHandler = func(p *migrator.MigrationProgress) {
		fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
			name, p.MigratedBlocks, p.TotalBlocks, p.MigratedBlocksPerc,
			p.ReadyBlocks, p.TotalBlocks, p.ReadyBlocksPerc,
			p.ActiveBlocks)
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
		if blocks == nil {
			break
		}

		// Optional: Send the list of dirty blocks over...
		dest.DirtyList(blocks)

		fmt.Printf("[%s] Migrating dirty blocks %d\n", name, len(blocks))
		err := mig.MigrateDirty(blocks)
		if err != nil {
			return err
		}
	}

	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	fmt.Printf("[%s] Migration completed\n", name)
	err = dest.SendEvent(protocol.EventCompleted)
	if err != nil {
		return err
	}
	return nil
}

func src_dev_setup(prov storage.StorageProvider) (storage.ExposedStorage, error) {
	p := expose.NewExposedStorageNBDNL(prov, 1, 0, prov.Size(), 4096, true)

	err := p.Init()
	if err != nil {
		fmt.Printf("p.Init returned %v\n", err)
		return nil, err
	}

	device := p.Device()
	fmt.Printf("* Device ready on /dev/%s\n", device)

	if serve_mount_dev {
		err = os.Mkdir(fmt.Sprintf("/mnt/mount%s", device), 0600)
		if err != nil {
			return nil, fmt.Errorf("Error mkdir %v", err)
		}

		cmd := exec.Command("mkfs.ext4", fmt.Sprintf("/dev/%s", device))
		err = cmd.Run()
		if err != nil {
			return nil, fmt.Errorf("Error mkfs.ext4 %v", err)
		}

		cmd = exec.Command("mount", fmt.Sprintf("/dev/%s", device), fmt.Sprintf("/mnt/mount%s", device))
		err = cmd.Run()
		if err != nil {
			return nil, fmt.Errorf("Error mount %v", err)
		}
	}
	return p, nil
}

func src_dev_shutdown(p storage.ExposedStorage) error {
	device := p.Device()

	if serve_mount_dev {

		cmd := exec.Command("umount", fmt.Sprintf("/dev/%s", device))
		err := cmd.Run()
		if err != nil {
			fmt.Printf("Could not umount device %s %v\n", device, err)
			//		return err
		}

		err = os.Remove(fmt.Sprintf("/mnt/mount%s", device))

		if err != nil {
			fmt.Printf("Count not remove /mnt/mount%s %v\n", device, err)
			//		return err
		}

		fmt.Printf("Shutdown nbd device %s\n", device)
		err = p.Shutdown()
		if err != nil {
			return err
		}

	}
	return nil
}
