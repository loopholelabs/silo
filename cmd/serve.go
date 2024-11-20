package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/loopholelabs/logging"
	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	siloprom "github.com/loopholelabs/silo/pkg/storage/metrics/prometheus"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
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
var serveProgress bool
var serveContinuous bool
var serveAnyOrder bool
var serveCompress bool

var serveMetrics string

var srcExposed []storage.ExposedStorage
var srcStorage []*storageInfo

var serveProgressBar *mpb.Progress
var serveBars []*mpb.Bar

var serveDebug bool

func init() {
	rootCmd.AddCommand(cmdServe)
	cmdServe.Flags().StringVarP(&serveAddr, "addr", "a", ":5170", "Address to serve from")
	cmdServe.Flags().StringVarP(&serveConf, "conf", "c", "silo.conf", "Configuration file")
	cmdServe.Flags().BoolVarP(&serveProgress, "progress", "p", false, "Show progress")
	cmdServe.Flags().BoolVarP(&serveContinuous, "continuous", "C", false, "Continuous sync")
	cmdServe.Flags().BoolVarP(&serveAnyOrder, "order", "o", false, "Any order (faster)")
	cmdServe.Flags().BoolVarP(&serveDebug, "debug", "d", false, "Debug logging (trace)")
	cmdServe.Flags().StringVarP(&serveMetrics, "metrics", "m", "", "Prom metrics address")
	cmdServe.Flags().BoolVarP(&serveCompress, "compress", "x", false, "Compress")
}

type storageInfo struct {
	//	tracker       storage.TrackingStorageProvider
	tracker   *dirtytracker.Remote
	lockable  storage.LockableProvider
	orderer   *blocks.PriorityBlockOrder
	numBlocks int
	blockSize int
	name      string
	schema    string
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

		siloMetrics = siloprom.New(reg)

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

	if serveProgress {
		serveProgressBar = mpb.New(
			mpb.WithOutput(color.Output),
			mpb.WithAutoRefresh(),
		)
		serveBars = make([]*mpb.Bar, 0)
	}

	srcExposed = make([]storage.ExposedStorage, 0)
	srcStorage = make([]*storageInfo, 0)
	fmt.Printf("Starting silo serve %s\n", serveAddr)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		shutdownEverything(log)
		os.Exit(1)
	}()

	siloConf, err := config.ReadSchema(serveConf)
	if err != nil {
		panic(err)
	}

	for i, s := range siloConf.Device {
		fmt.Printf("Setup storage %d [%s] size %s - %d\n", i, s.Name, s.Size, s.ByteSize())
		sinfo, err := setupStorageDevice(s, log, siloMetrics)
		if err != nil {
			panic(fmt.Sprintf("Could not setup storage. %v", err))
		}

		srcStorage = append(srcStorage, sinfo)
	}

	// Setup listener here. When client connects, migrate data to it.

	l, err := net.Listen("tcp", serveAddr)
	if err != nil {
		shutdownEverything(log)
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

		// Lets go through each of the things we want to migrate...
		ctime := time.Now()

		var wg sync.WaitGroup

		for i, s := range srcStorage {
			wg.Add(1)
			go func(index int, src *storageInfo) {
				err := migrateDevice(log, siloMetrics, uint32(index), src.name, pro, src)
				if err != nil {
					fmt.Printf("There was an issue migrating the storage %d %v\n", index, err)
				}
				wg.Done()
			}(i, s)
		}
		wg.Wait()

		if serveProgressBar != nil {
			serveProgressBar.Wait()
		}
		fmt.Printf("\n\nMigration completed in %dms\n", time.Since(ctime).Milliseconds())

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
	shutdownEverything(log)
}

func shutdownEverything(log types.Logger) {
	// first unlock everything
	fmt.Printf("Unlocking devices...\n")
	for _, i := range srcStorage {
		i.lockable.Unlock()
		i.tracker.Close()
	}

	fmt.Printf("Shutting down devices cleanly...\n")
	for _, p := range srcExposed {
		device := p.Device()

		fmt.Printf("Shutdown nbd device %s\n", device)
		_ = p.Shutdown()

		// Show some metrics...
		if log != nil {
			nbdDevice, ok := p.(*expose.ExposedStorageNBDNL)
			if ok {
				m := nbdDevice.GetMetrics()
				log.Debug().
					Uint64("PacketsIn", m.PacketsIn).
					Uint64("PacketsOut", m.PacketsOut).
					Uint64("ReadAt", m.ReadAt).
					Uint64("ReadAtBytes", m.ReadAtBytes).
					Uint64("ReadAtTimeMS", uint64(m.ReadAtTime.Milliseconds())).
					Uint64("WriteAt", m.WriteAt).
					Uint64("WriteAtBytes", m.WriteAtBytes).
					Uint64("WriteAtTimeMS", uint64(m.WriteAtTime.Milliseconds())).
					Str("device", p.Device()).
					Msg("NBD metrics")
			}
		}
	}
}

func setupStorageDevice(conf *config.DeviceSchema, log types.Logger, met metrics.SiloMetrics) (*storageInfo, error) {
	source, ex, err := device.NewDeviceWithLoggingMetrics(conf, log, met)
	if err != nil {
		return nil, err
	}
	if ex != nil {
		fmt.Printf("Device %s exposed as %s\n", conf.Name, ex.Device())
		srcExposed = append(srcExposed, ex)
	}

	blockSize := 1024 * 128

	if conf.BlockSize != "" {
		blockSize = int(conf.ByteBlockSize())
	}

	numBlocks := (int(conf.ByteSize()) + blockSize - 1) / blockSize

	sourceMetrics := modules.NewMetrics(source)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceMetrics, blockSize)
	sourceMonitor := volatilitymonitor.NewVolatilityMonitor(sourceDirtyLocal, blockSize, 10*time.Second)
	sourceStorage := modules.NewLockable(sourceMonitor)

	if met != nil {
		met.AddDirtyTracker(conf.Name, sourceDirtyRemote)
		met.AddVolatilityMonitor(conf.Name, sourceMonitor)
		met.AddMetrics(conf.Name, sourceMetrics)
	}

	if ex != nil {
		ex.SetProvider(sourceStorage)
	}

	// Start monitoring blocks.

	var primaryOrderer storage.BlockOrder
	primaryOrderer = sourceMonitor

	if serveAnyOrder {
		primaryOrderer = blocks.NewAnyBlockOrder(numBlocks, nil)
	}
	orderer := blocks.NewPriorityBlockOrder(numBlocks, primaryOrderer)
	orderer.AddAll()

	schema := string(conf.Encode())

	sinfo := &storageInfo{
		tracker:   sourceDirtyRemote,
		lockable:  sourceStorage,
		orderer:   orderer,
		blockSize: blockSize,
		numBlocks: numBlocks,
		name:      conf.Name,
		schema:    schema,
	}

	return sinfo, nil
}

// Migrate a device
func migrateDevice(log types.Logger, met metrics.SiloMetrics, devID uint32, name string,
	pro protocol.Protocol,
	sinfo *storageInfo) error {
	size := sinfo.lockable.Size()
	dest := protocol.NewToProtocol(size, devID, pro)

	// Maybe compress writes
	dest.CompressedWrites = serveCompress

	err := dest.SendDevInfo(name, uint32(sinfo.blockSize), sinfo.schema)
	if err != nil {
		return err
	}

	statusString := " "

	statusFn := func(_ decor.Statistics) string {
		return statusString
	}

	var bar *mpb.Bar
	if serveProgress {
		bar = serveProgressBar.AddBar(int64(size),
			mpb.PrependDecorators(
				decor.Name(name, decor.WCSyncSpaceR),
				decor.CountersKiloByte("%d/%d", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(
				decor.EwmaETA(decor.ET_STYLE_GO, 30),
				decor.Name(" "),
				decor.EwmaSpeed(decor.SizeB1024(0), "% .2f", 60, decor.WCSyncWidth),
				decor.OnComplete(decor.Percentage(decor.WC{W: 5}), "done"),
				decor.Name(" "),
				decor.Any(statusFn, decor.WC{W: 2}),
			),
		)

		serveBars = append(serveBars, bar)
	}

	go func() {
		_ = dest.HandleNeedAt(func(offset int64, length int32) {
			// Prioritize blocks...
			end := uint64(offset + int64(length))
			if end > size {
				end = size
			}

			bStart := int(offset / int64(sinfo.blockSize))
			bEnd := int((end-1)/uint64(sinfo.blockSize)) + 1
			for b := bStart; b < bEnd; b++ {
				// Ask the orderer to prioritize these blocks...
				sinfo.orderer.PrioritiseBlock(b)
			}
		})
	}()

	go func() {
		_ = dest.HandleDontNeedAt(func(offset int64, length int32) {
			end := uint64(offset + int64(length))
			if end > size {
				end = size
			}

			bStart := int(offset / int64(sinfo.blockSize))
			bEnd := int((end-1)/uint64(sinfo.blockSize)) + 1
			for b := bStart; b < bEnd; b++ {
				sinfo.orderer.Remove(b)
			}
		})
	}()

	conf := migrator.NewConfig().WithBlockSize(sinfo.blockSize)
	conf.Logger = log
	conf.LockerHandler = func() {
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPreLock})
		sinfo.lockable.Lock()
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPostLock})
	}
	conf.UnlockerHandler = func() {
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPreUnlock})
		sinfo.lockable.Unlock()
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPostUnlock})
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 1000,
	}
	conf.ErrorHandler = func(_ *storage.BlockInfo, err error) {
		// For now...
		panic(err)
	}
	conf.Integrity = true

	lastValue := uint64(0)
	lastTime := time.Now()

	if serveProgress {

		conf.ProgressHandler = func(p *migrator.MigrationProgress) {
			v := uint64(p.ReadyBlocks) * uint64(sinfo.blockSize)
			if v > size {
				v = size
			}
			bar.SetCurrent(int64(v))
			bar.EwmaIncrInt64(int64(v-lastValue), time.Since(lastTime))
			lastTime = time.Now()
			lastValue = v
		}
	} else {
		conf.ProgressHandler = func(p *migrator.MigrationProgress) {
			fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
				name, p.MigratedBlocks, p.TotalBlocks, p.MigratedBlocksPerc,
				p.ReadyBlocks, p.TotalBlocks, p.ReadyBlocksPerc,
				p.ActiveBlocks)
		}
		conf.ErrorHandler = func(b *storage.BlockInfo, err error) {
			fmt.Printf("[%s] Error for block %d error %v\n", name, b.Block, err)
		}
	}

	mig, err := migrator.NewMigrator(sinfo.tracker, dest, sinfo.orderer, conf)

	if err != nil {
		return err
	}

	if met != nil {
		met.AddToProtocol(name, dest)
		met.AddMigrator(name, mig)
	}

	migrateBlocks := sinfo.numBlocks

	// Now do the migration...
	err = mig.Migrate(migrateBlocks)
	if err != nil {
		return err
	}

	// Wait for completion.
	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	hashes := mig.GetHashes() // Get the initial hashes and send them over for verification...
	err = dest.SendHashes(hashes)
	if err != nil {
		return err
	}

	// Optional: Enter a loop looking for more dirty blocks to migrate...
	for {
		blocks := mig.GetLatestDirty() //
		if !serveContinuous && blocks == nil {
			break
		}

		if blocks != nil {
			// Optional: Send the list of dirty blocks over...
			err := dest.DirtyList(conf.BlockSize, blocks)
			if err != nil {
				return err
			}

			//		fmt.Printf("[%s] Migrating dirty blocks %d\n", name, len(blocks))
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

	//	fmt.Printf("[%s] Migration completed\n", name)

	err = dest.SendEvent(&packets.Event{Type: packets.EventCompleted})
	if err != nil {
		return err
	}
	/*
		// Completed.
		if serve_progress {
			//		bar.SetCurrent(int64(size))
			//		bar.EwmaIncrInt64(int64(size-last_value), time.Since(last_time))
		}
	*/

	if log != nil {
		toMetrics := dest.GetMetrics()
		log.Debug().
			Str("name", name).
			Uint64("SentEvents", toMetrics.SentEvents).
			Uint64("SentHashes", toMetrics.SentHashes).
			Uint64("SentDevInfo", toMetrics.SentDevInfo).
			Uint64("SentRemoveDev", toMetrics.SentRemoveDev).
			Uint64("SentDirtyList", toMetrics.SentDirtyList).
			Uint64("SentReadAt", toMetrics.SentReadAt).
			Uint64("SentWriteAtHash", toMetrics.SentWriteAtHash).
			Uint64("SentWriteAtComp", toMetrics.SentWriteAtComp).
			Uint64("SentWriteAt", toMetrics.SentWriteAt).
			Uint64("SentWriteAtWithMap", toMetrics.SentWriteAtWithMap).
			Uint64("SentRemoveFromMap", toMetrics.SentRemoveFromMap).
			Uint64("SentNeedAt", toMetrics.RecvNeedAt).
			Uint64("SentDontNeedAt", toMetrics.RecvDontNeedAt).
			Msg("ToProtocol metrics")
	}

	return nil
}
