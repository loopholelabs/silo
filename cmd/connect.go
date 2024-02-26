package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

func init() {
	rootCmd.AddCommand(cmdConnect)
	cmdConnect.Flags().StringVarP(&connect_addr, "addr", "a", "localhost:5170", "Address to serve from")
	cmdConnect.Flags().StringVarP(&connect_dev, "dev", "d", "", "Device eg nbd1")
}

func runConnect(ccmd *cobra.Command, args []string) {
	fmt.Printf("Starting silo connect %s at %s\n", connect_dev, connect_addr)

	// Setup some statistics output
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":4114", nil)

	block_size := 1024 * 64

	var p storage.ExposedStorage

	var destStorage storage.StorageProvider
	var destWaitingLocal *modules.WaitingCacheLocal
	var destWaitingRemote *modules.WaitingCacheRemote
	var destStorageMetrics *modules.Metrics

	con, err := net.Dial("tcp", connect_addr)
	if err != nil {
		panic("Error connecting")
	}

	var dest *modules.FromProtocol

	destWaitingRemoteFactory := func(di *protocol.DevInfo) storage.StorageProvider {
		fmt.Printf("Received DevInfo size=%d\n", di.Size)
		cr := func(s int) storage.StorageProvider {
			return sources.NewMemoryStorage(s)
		}
		// Setup some sharded memory storage (for concurrent write speed)
		destStorage = modules.NewShardedStorage(int(di.Size), int(di.Size/1024), cr)
		// Wrap it in metrics
		destWaitingLocal, destWaitingRemote = modules.NewWaitingCache(destStorage, block_size)
		destStorageMetrics = modules.NewMetrics(destWaitingLocal)

		// Set something up to randomly read...
		go func() {
			for {

				o := rand.Intn(int(di.Size))

				b := make([]byte, 1)

				//			rtime := time.Now()
				destStorageMetrics.ReadAt(b, int64(o))

				//			fmt.Printf("DATA READ %d %d %v %dms\n", o, n, err, time.Since(rtime).Milliseconds())

				w := rand.Intn(10)

				time.Sleep(time.Duration(w) * time.Millisecond)
			}
		}()

		// Connect the waitingCache to the FromProtocol
		destWaitingLocal.NeedAt = func(offset int64, length int32) {
			dest.NeedAt(offset, length)
		}

		destWaitingLocal.DontNeedAt = func(offset int64, length int32) {
			dest.DontNeedAt(offset, length)
		}
		return destWaitingRemote
	}

	pro := protocol.NewProtocolRW(context.TODO(), con, con)
	dest = modules.NewFromProtocol(777, destWaitingRemoteFactory, pro)

	go func() {
		err := pro.Handle()
		fmt.Printf("PROTOCOL ERROR %v\n", err)

		// If it's EOF then the migration is completed

	}()

	go dest.HandleSend(context.TODO())
	go dest.HandleReadAt()
	go dest.HandleWriteAt()
	go dest.HandleDevInfo()
	go dest.HandleEvent(func(e protocol.EventType) {
		fmt.Printf("= Event %s\n", protocol.EventsByType[e])
	})

	go dest.HandleDirtyList(func(dirty []uint) {
		fmt.Printf("GOT LIST OF DIRTY BLOCKS %v\n", dirty)
	})

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
		p, err = setup(connect_dev, destStorageMetrics, false)
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
