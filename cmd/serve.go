package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loopholelabs/logging"
	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/devicegroup"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	siloprom "github.com/loopholelabs/silo/pkg/storage/metrics/prometheus"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
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

var serveAddr string
var serveConf string
var serveContinuous bool

var serveMetrics string
var serveDebug bool

func init() {
	rootCmd.AddCommand(cmdServe)
	cmdServe.Flags().StringVarP(&serveAddr, "addr", "a", ":5170", "Address to serve from")
	cmdServe.Flags().StringVarP(&serveConf, "conf", "c", "silo.conf", "Configuration file")
	cmdServe.Flags().BoolVarP(&serveDebug, "debug", "d", false, "Debug logging (trace)")
	cmdServe.Flags().StringVarP(&serveMetrics, "metrics", "m", "", "Prom metrics address")
	cmdServe.Flags().BoolVarP(&serveContinuous, "continuous", "C", false, "Continuous sync")
}

func runServe(_ *cobra.Command, _ []string) {
	var log types.RootLogger
	var reg *prometheus.Registry
	var siloMetrics metrics.SiloMetrics

	if serveDebug {
		log = logging.New(logging.Zerolog, "silo.serve", os.Stderr)
		log.SetLevel(types.TraceLevel)
	}

	if serveMetrics != "" {
		reg = prometheus.NewRegistry()

		siloMetrics = siloprom.New(reg, siloprom.DefaultConfig())

		// Add the default go metrics
		reg.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)

		http.Handle("/metrics", promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				// Opt into OpenMetrics to support exemplars.
				EnableOpenMetrics: true,
				// Pass custom registry
				Registry: reg,
			},
		))

		go http.ListenAndServe(serveMetrics, nil)
	}

	fmt.Printf("Starting silo serve %s\n", serveAddr)

	siloConf, err := config.ReadSchema(serveConf)
	if err != nil {
		panic(err)
	}

	dg, err := devicegroup.NewFromSchema(siloConf.Device, false, log, siloMetrics)
	if err != nil {
		panic(err)
	}

	for _, d := range siloConf.Device {
		expName := dg.GetExposedDeviceByName(d.Name)
		if expName != nil {
			fmt.Printf("Device %s exposed at %s\n", d.Name, expName.Device())
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		dg.CloseAll()
		os.Exit(1)
	}()

	// Setup listener here. When client connects, migrate data to it.
	l, err := net.Listen("tcp", serveAddr)
	if err != nil {
		dg.CloseAll()
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

		if siloMetrics != nil {
			siloMetrics.AddProtocol("serve", pro)
		}

		ctime := time.Now()

		// Migrate everything to the destination...
		err = dg.StartMigrationTo(pro, true)
		if err != nil {
			dg.CloseAll()
			panic(err)
		}

		err = dg.MigrateAll(1000, func(ps map[string]*migrator.MigrationProgress) {
			for name, p := range ps {
				fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
					name, p.MigratedBlocks, p.TotalBlocks, p.MigratedBlocksPerc,
					p.ReadyBlocks, p.TotalBlocks, p.ReadyBlocksPerc,
					p.ActiveBlocks)
			}
		})
		if err != nil {
			dg.CloseAll()
			panic(err)
		}

		fmt.Printf("All devices migrated in %dms.\n", time.Since(ctime).Milliseconds())

		// Now do a dirty block phase...
		hooks := &devicegroup.MigrateDirtyHooks{
			PreGetDirty: func(name string) error {
				fmt.Printf("# [%s]PreGetDirty\n", name)
				return nil
			},
			PostGetDirty: func(name string, blocks []uint) (bool, error) {
				fmt.Printf("# [%s]PostGetDirty %d\n", name, len(blocks))
				if serveContinuous {
					return true, nil
				}
				return len(blocks) > 0, nil
			},
			PostMigrateDirty: func(name string, blocks []uint) (bool, error) {
				fmt.Printf("# [%s]PostMigrateDirty %d\n", name, len(blocks))
				time.Sleep(1 * time.Second) // Wait a bit for next dirty loop
				return true, nil
			},
			Completed: func(name string) {
				fmt.Printf("# [%s]Completed\n", name)
			},
		}
		err = dg.MigrateDirty(hooks)
		if err != nil {
			dg.CloseAll()
			panic(err)
		}

		fmt.Printf("All devices migrated(including dirty) in %dms.\n", time.Since(ctime).Milliseconds())

		err = dg.Completed() // Send completion events for the devices.
		if err != nil {
			dg.CloseAll()
			panic(err)
		}

		if log != nil {
			metrics := pro.GetMetrics()
			log.Debug().
				Uint64("PacketsSent", metrics.PacketsSent).
				Uint64("DataSent", metrics.DataSent).
				Uint64("PacketsRecv", metrics.PacketsRecv).
				Uint64("DataRecv", metrics.DataRecv).
				Msg("protocol metrics")
		}

		con.Close()
	}
	dg.CloseAll()
}
