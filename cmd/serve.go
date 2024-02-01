package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/internal/expose"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
var serve_dev string
var serve_size int

func init() {
	rootCmd.AddCommand(cmdServe)
	cmdServe.Flags().StringVarP(&serve_addr, "addr", "a", ":5170", "Address to serve from")
	cmdServe.Flags().StringVarP(&serve_dev, "dev", "d", "", "Device eg nbd1")
	cmdServe.Flags().IntVarP(&serve_size, "size", "s", 1024*1024*10, "Size")
}

var (
	prom_read_ops    = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_read_ops", Help: "silo_read_ops"})
	prom_read_bytes  = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_read_bytes", Help: "silo_read_bytes"})
	prom_read_time   = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_read_time", Help: "silo_read_time"})
	prom_read_errors = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_read_errors", Help: "silo_read_errors"})

	prom_write_ops    = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_write_ops", Help: "silo_write_ops"})
	prom_write_bytes  = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_write_bytes", Help: "silo_write_bytes"})
	prom_write_time   = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_write_time", Help: "silo_write_time"})
	prom_write_errors = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_write_errors", Help: "silo_write_errors"})

	prom_flush_ops    = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_flush_ops", Help: "silo_flush_ops"})
	prom_flush_time   = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_flush_time", Help: "silo_flush_time"})
	prom_flush_errors = promauto.NewGauge(prometheus.GaugeOpts{Name: "silo_flush_errors", Help: "silo_flush_errors"})
)

func runServe(ccmd *cobra.Command, args []string) {
	fmt.Printf("Starting silo serve %s at %s size %d\n", serve_dev, serve_addr, serve_size)

	// Setup some statistics output
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":2112", nil)

	block_size := 4096
	num_blocks := (serve_size + block_size - 1) / block_size

	var p storage.ExposedStorage

	cr := func(s int) storage.StorageProvider {
		return sources.NewMemoryStorage(s)
	}
	// Setup some sharded memory storage (for concurrent write speed)
	source := modules.NewShardedStorage(serve_size, serve_size/1024, cr)
	// Wrap it in metrics

	sourceMetrics := modules.NewMetrics(source)
	sourceDirty := modules.NewFilterReadDirtyTracker(sourceMetrics, block_size)
	sourceMonitor := modules.NewVolatilityMonitor(sourceDirty, block_size, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	// Start monitoring blocks.
	orderer := blocks.NewPriorityBlockOrder(num_blocks, sourceMonitor)

	for i := 0; i < num_blocks; i++ {
		orderer.Add(i)
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c

		if serve_dev != "" {
			fmt.Printf("\nShutting down cleanly...\n")
			shutdown(serve_dev, p)
		}
		sourceMetrics.ShowStats("Source")
		os.Exit(1)
	}()

	if serve_dev != "" {
		var err error
		d := expose.NewDispatch()
		p, err = setup(serve_dev, d, sourceStorage, true)
		if err != nil {
			fmt.Printf("Error during setup %v\n", err)
			return
		}
		fmt.Printf("Ready...\n")
	}

	// TODO: Setup listener here. When client connects, migrate to it.

	l, err := net.Listen("tcp", serve_addr)
	if err != nil {
		if serve_dev != "" {
			fmt.Printf("\nShutting down cleanly...\n")
			shutdown(serve_dev, p)
		}
		panic("Listener issue...")
	}

	go func() {
		fmt.Printf("Waiting for connection...\n")
		c, err := l.Accept()
		if err == nil {
			fmt.Printf("GOT CONNECTION\n")
			// Now we can migrate to the client...

			locker := func() {
				// This could be used to pause VM/consumer etc...
				sourceStorage.Lock()
			}
			unlocker := func() {
				// Restart consumer
				sourceStorage.Unlock()
			}

			pro := protocol.NewProtocolRW(context.TODO(), c, c)
			dest := modules.NewToProtocol(uint64(serve_size), 777, pro)

			go pro.Handle()

			mig := storage.NewMigrator(sourceDirty,
				dest,
				block_size,
				locker,
				unlocker,
				orderer)

			// Now do the migration...
			err = mig.Migrate()
			fmt.Printf("MIGRATION DONE %v\n", err)
		}
	}()

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			// Show some stats...
			sourceMetrics.ShowStats("Source")
			fmt.Printf("Volatility %d\n", sourceMonitor.GetTotalVolatility())

			s := sourceMetrics.Snapshot()
			prom_read_ops.Set(float64(s.Read_ops))
			prom_read_bytes.Set(float64(s.Read_bytes))
			prom_read_time.Set(float64(s.Read_time))
			prom_read_errors.Set(float64(s.Read_errors))

			prom_write_ops.Set(float64(s.Write_ops))
			prom_write_bytes.Set(float64(s.Write_bytes))
			prom_write_time.Set(float64(s.Write_time))
			prom_write_errors.Set(float64(s.Write_errors))

			prom_flush_ops.Set(float64(s.Flush_ops))
			prom_flush_time.Set(float64(s.Flush_time))
			prom_flush_errors.Set(float64(s.Flush_errors))

		}
	}
}

/**
 * Setup a disk
 *
 */
func setup(device string, dispatch expose.NBDDispatcher, prov storage.StorageProvider, server bool) (storage.ExposedStorage, error) {
	p, err := expose.NewNBD(dispatch, fmt.Sprintf("/dev/%s", device))
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

	err = os.Mkdir(fmt.Sprintf("/mnt/mount%s", device), 0600)
	if err != nil {
		return nil, fmt.Errorf("Error mkdir %v", err)
	}

	if server {
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
	} else {
		cmd := exec.Command("mount", "-r", fmt.Sprintf("/dev/%s", device), fmt.Sprintf("/mnt/mount%s", device))
		err = cmd.Run()
		if err != nil {
			return nil, fmt.Errorf("Error mount %v", err)
		}
	}

	return p, nil
}

func shutdown(device string, p storage.ExposedStorage) error {
	fmt.Printf("shutdown %s\n", device)
	cmd := exec.Command("umount", fmt.Sprintf("/dev/%s", device))
	err := cmd.Run()
	if err != nil {
		return err
	}
	err = os.Remove(fmt.Sprintf("/mnt/mount%s", device))
	if err != nil {
		return err
	}

	err = p.Shutdown()
	if err != nil {
		return err
	}
	return nil
}
