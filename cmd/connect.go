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
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var (
	cmdConnect = &cobra.Command{
		Use:   "connect",
		Short: "Conndct and stream Silo devices.",
		Long:  `Connect to a Silo instance, and stream available devices.`,
		Run:   runConnect,
	}
)

// Address to connect to
var connectAddr string

var connectDebug bool
var connectMetrics string

func init() {
	rootCmd.AddCommand(cmdConnect)
	cmdConnect.Flags().StringVarP(&connectAddr, "addr", "a", "localhost:5170", "Address to serve from")
	cmdConnect.Flags().BoolVarP(&connectDebug, "debug", "d", false, "Debug logging (trace)")
	cmdConnect.Flags().StringVarP(&connectMetrics, "metrics", "M", "", "Prom metrics address")
}

/**
 * Connect to a silo source and stream whatever devices are available.
 *
 */
func runConnect(_ *cobra.Command, _ []string) {
	var log types.RootLogger
	var reg *prometheus.Registry
	var siloMetrics metrics.SiloMetrics

	if connectDebug {
		log = logging.New(logging.Zerolog, "silo.connect", os.Stderr)
		log.SetLevel(types.TraceLevel)
	}

	if connectMetrics != "" {
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

		go http.ListenAndServe(connectMetrics, nil)
	}

	fmt.Printf("Starting silo connect from source %s\n", connectAddr)

	var dg *devicegroup.DeviceGroup

	// Handle shutdown gracefully to disconnect any exposed devices correctly.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		if dg != nil {
			dg.CloseAll()
		}
		os.Exit(1)
	}()

	// Connect to the source
	fmt.Printf("Dialing source at %s...\n", connectAddr)
	con, err := net.Dial("tcp", connectAddr)
	if err != nil {
		panic("Error connecting to source")
	}

	// Wrap the connection in a protocol, and handle incoming devices

	protoCtx, protoCancelfn := context.WithCancel(context.TODO())

	pro := protocol.NewRW(protoCtx, []io.Reader{con}, []io.Writer{con}, nil)

	// Let the protocol do its thing.
	go func() {
		err = pro.Handle()
		if err != nil && err != io.EOF {
			fmt.Printf("Silo protocol error %v\n", err)
			return
		}
		// We should get an io.EOF here once the migrations have all completed.
		// We should cancel the context, to cancel anything that is waiting for packets.
		protoCancelfn()
	}()

	if siloMetrics != nil {
		siloMetrics.AddProtocol("protocol", pro)
	}

	// TODO: Modify schemas a bit here...
	tweak := func(_ int, _ string, schema *config.DeviceSchema) *config.DeviceSchema {
		return schema
	}

	dg, err = devicegroup.NewFromProtocol(protoCtx, pro, tweak, nil, nil, log, siloMetrics)

	for _, d := range dg.GetDeviceSchema() {
		expName := dg.GetExposedDeviceByName(d.Name)
		if expName != nil {
			fmt.Printf("Device %s exposed at %s\n", d.Name, expName.Device())
		}
	}

	// Wait for completion events.
	dg.WaitForCompletion()

	fmt.Printf("\nMigrations completed. Please ctrl-c if you want to shut down, or wait an hour :)\n")

	// We should pause here, to allow the user to do things with the devices
	time.Sleep(1 * time.Hour)

	// Shutdown any storage exposed as devices
	dg.CloseAll()
}
