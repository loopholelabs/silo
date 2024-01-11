package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/internal/expose"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

const device = "/dev/nbd12"
const mountpoint = "/mnt/testNBD12"

func main() {

	size := 1024 * 1024 * 1024 * 1

	var p storage.ExposedStorage

	cr := func(s int) storage.StorageProvider {
		return sources.NewMemoryStorage(s)
	}
	// Setup some sharded memory storage (for concurrent write speed)
	source := sources.NewShardedStorage(size, size/1024, cr)
	// Wrap it in metrics
	metricsSource := modules.NewMetrics(source)
	// Wrap that in a dirty tracker
	dirty := modules.NewFilterDirtyTracker(metricsSource)
	// Wrap in a filter for redundant writes
	filtered := modules.NewFilterRedundantWrites(dirty, source, 32)
	// Wrap that in metrics
	metrics := modules.NewMetrics(filtered)
	// Wrap that in a splitter so we can add a cache in when we want to
	driver := modules.NewSplitter(metrics)

	// Create a cache backed by memory, with some metrics
	exists := sources.NewMemoryStorage(size)
	memCache := modules.NewMetrics(sources.NewMemoryStorage(size))
	cache := modules.NewCache(memCache, exists)

	// Later...
	// driver.AddProvider(cache)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c

		fmt.Printf("\nShutting down cleanly...\n")
		shutdown(p)
		wis := modules.CleanupWriteInfo(dirty.Sync())
		for _, wi := range wis {
			fmt.Printf("Dirty data %d %d\n", wi.Offset, wi.Length)
		}
		metricsSource.ShowStats("Source")
		metrics.ShowStats("Driver")
		memCache.ShowStats("Cache")
		cache.ShowStats("HitRate")
		// Run Cleanup here...
		os.Exit(1)
	}()

	d := expose.NewDispatch()
	p, err := setup(d, driver)

	// Add a cache NOW
	driver.AddProvider(cache)

	dirty.Track()

	if err != nil {
		fmt.Printf("Error setup %v\n", err)
		return
	}

	fmt.Printf("Ready...\n")

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			// Show some stats...
			metricsSource.ShowStats("Source")
			metrics.ShowStats("Driver")
			memCache.ShowStats("Cache")
			cache.ShowStats("HitRate")
		}
	}
}

/**
 * Setup a disk with some files created.
 *
 */
func setup(dispatch expose.NBDDispatcher, prov storage.StorageProvider) (storage.ExposedStorage, error) {
	p, err := expose.NewNBD(dispatch, device)
	if err != nil {
		return nil, err
	}

	go func() {
		err := p.Handle(prov)
		if err != nil {
			fmt.Printf("p.Handle returned %v\n", err)
		}
	}()

	p.WaitReady()

	err = os.Mkdir(mountpoint, 0600)
	if err != nil {
		return nil, fmt.Errorf("Error mkdir %v", err)
	}

	cmd := exec.Command("mkfs.ext4", device)
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("Error mkfs.ext4 %v", err)
	}

	cmd = exec.Command("mount", device, mountpoint)
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("Error mount %v", err)
	}

	return p, nil
}

func shutdown(p storage.ExposedStorage) error {
	fmt.Printf("shutdown %s %s\n", device, mountpoint)
	cmd := exec.Command("umount", device)
	err := cmd.Run()
	if err != nil {
		return err
	}
	err = os.Remove(mountpoint)
	if err != nil {
		return err
	}

	err = p.Shutdown()
	if err != nil {
		return err
	}
	return nil
}
