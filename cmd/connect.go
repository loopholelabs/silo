package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/internal/expose"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var (
	cmdConnect = &cobra.Command{
		Use:   "connect",
		Short: "Start up connect",
		Long:  ``,
		Run:   runConnect,
	}
)

var connect_addr string
var connect_dev string
var connect_size int

func init() {
	rootCmd.AddCommand(cmdConnect)
	cmdConnect.Flags().StringVarP(&connect_addr, "addr", "a", "localhost:5170", "Address to serve from")
	cmdConnect.Flags().StringVarP(&connect_dev, "dev", "d", "", "Device eg nbd1")
	cmdConnect.Flags().IntVarP(&connect_size, "size", "s", 1024*1024*10, "Size")
}

func runConnect(ccmd *cobra.Command, args []string) {
	fmt.Printf("Starting silo connect %s at %s size %d\n", connect_dev, connect_addr, connect_size)

	// Setup some statistics output
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":4114", nil)

	block_size := 4096

	var p storage.ExposedStorage

	cr := func(s int) storage.StorageProvider {
		return sources.NewMemoryStorage(s)
	}
	// Setup some sharded memory storage (for concurrent write speed)
	destStorage := modules.NewShardedStorage(connect_size, connect_size/1024, cr)
	// Wrap it in metrics
	destWaiting := modules.NewWaitingCache(destStorage, block_size)
	destStorageMetrics := modules.NewMetrics(destWaiting)

	con, err := net.Dial("tcp", connect_addr)
	if err != nil {
		panic("Error connecting")
	}

	pro := protocol.NewProtocolRW(context.TODO(), con, con)
	dest := modules.NewFromProtocol(777, destStorageMetrics, pro)

	go pro.Handle()

	go dest.HandleSend(context.TODO())
	go dest.HandleReadAt()
	go dest.HandleWriteAt()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c

		if connect_dev != "" {
			fmt.Printf("\nShutting down cleanly...\n")
			shutdown(connect_dev, p)
		}
		destStorageMetrics.ShowStats("Source")
		os.Exit(1)
	}()

	if connect_dev != "" {
		d := expose.NewDispatch()
		p, err = setup(connect_dev, d, destStorageMetrics, false)
		if err != nil {
			fmt.Printf("Error during setup %v\n", err)
			return
		}
		fmt.Printf("Ready on %s...\n", connect_dev)
	}

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			// Show some stats...
			destStorageMetrics.ShowStats("Dest")

			s := destStorageMetrics.Snapshot()
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