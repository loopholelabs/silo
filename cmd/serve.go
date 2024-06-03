package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/device"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
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

var serve_addr string
var serve_conf string
var serve_progress bool
var serve_continuous bool
var serve_any_order bool
var serve_pageserve_addr string

var src_exposed []storage.ExposedStorage
var src_storage []*storageInfo

var serveProgress *mpb.Progress
var serveBars []*mpb.Bar

func init() {
	rootCmd.AddCommand(cmdServe)
	cmdServe.Flags().StringVarP(&serve_addr, "addr", "a", ":5170", "Address to serve from")
	cmdServe.Flags().StringVarP(&serve_conf, "conf", "c", "silo.conf", "Configuration file")
	cmdServe.Flags().BoolVarP(&serve_progress, "progress", "p", false, "Show progress")
	cmdServe.Flags().BoolVarP(&serve_continuous, "continuous", "C", false, "Continuous sync")
	cmdServe.Flags().BoolVarP(&serve_any_order, "order", "o", false, "Any order (faster)")
	cmdServe.Flags().StringVarP(&serve_pageserve_addr, "pagesaddr", "A", ":5171", "Address to accept criu page data from")
}

type storageInfo struct {
	//	tracker       storage.TrackingStorageProvider
	tracker       *dirtytracker.DirtyTrackerRemote
	lockable      storage.LockableStorageProvider
	orderer       *blocks.PriorityBlockOrder
	num_blocks    int
	block_size    int
	name          string
	mapped        *modules.MappedStorage
	schema        string
	data_complete func()
}

func runServe(ccmd *cobra.Command, args []string) {
	if serve_progress {
		serveProgress = mpb.New(
			mpb.WithOutput(color.Output),
			mpb.WithAutoRefresh(),
		)
		serveBars = make([]*mpb.Bar, 0)
	}

	src_exposed = make([]storage.ExposedStorage, 0)
	src_storage = make([]*storageInfo, 0)
	fmt.Printf("Starting silo serve %s\n", serve_addr)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		shutdown_everything()
		os.Exit(1)
	}()

	siloConf, err := config.ReadSchema(serve_conf)
	if err != nil {
		panic(err)
	}

	page_mapped := make(map[int]*storageInfo)

	static_page_data := criu.NewPageStore()

	for i, s := range siloConf.Device {
		fmt.Printf("Setup storage %d [%s] size %s - %d\n", i, s.Name, s.Size, s.ByteSize())
		sinfo, err := setupStorageDevice(s)
		if err != nil {
			panic(fmt.Sprintf("Could not setup storage. %v", err))
		}

		// Put it in the map for pageserver here...
		if s.PageServerPID != 0 {
			page_mapped[s.PageServerPID] = sinfo
		}

		src_storage = append(src_storage, sinfo)
	}

	// Setup page-server for receiving memory data...
	fmt.Printf("Waiting for page server connection on %s\n", serve_pageserve_addr)

	listener, err := net.Listen("tcp", serve_pageserve_addr)
	if err != nil {
		panic(err)
	}

	// Handle any incoming page server data...
	go func() {
		for {
			con, err := listener.Accept()
			if err != nil {
				panic(err)
			}

			is_pre := false
			pids := make(map[int]*storageInfo)
			var pending_writes sync.WaitGroup

			ps := criu.NewPageServer()
			ps.Close = func(iov *criu.PageServerIOV) {
				// Wait for any pending writes
				pending_writes.Wait()

				if !is_pre {
					for _, sinfo := range pids {
						// Here we mark that the data for these pids as FINAL, and migration can complete safely
						sinfo.data_complete()
					}

					// Dump the static pagemap data to files (outputs dir). This will be needed in a restore.
					// TODO: Send it over along with the rest of the dump data...
					static_page_data.Dump("outputs")
				}

			}
			ps.Open2 = func(iov *criu.PageServerIOV) bool {
				sinfo, ok := page_mapped[int(iov.DstID())]
				if !ok {
					return false
				}
				pids[int(iov.DstID())] = sinfo
				return sinfo.mapped.Size() > 0
			}
			ps.Parent = func(iov *criu.PageServerIOV) bool {
				is_pre = true
				sinfo, ok := page_mapped[int(iov.DstID())]
				if !ok {
					return false
				}
				return sinfo.mapped.Size() > 0
			}
			ps.GetPageData = func(iov *criu.PageServerIOV, buffer []byte) {
				// Unimplemented for now...
				fmt.Printf("[Unimplemented] GetPageData %v\n", iov)
			}
			ps.AddPageData = func(iov *criu.PageServerIOV, buffer []byte) {
				//				fmt.Printf("AddPageData %s %d\n", iov.String(), len(buffer))
				if iov.FlagsPresent() {
					maps, ok := page_mapped[int(iov.DstID())]
					if !ok {
						// The destination PID is unknown or unconfigured...
						fmt.Printf("Destination PID unknown %d\n", iov.DstID())
					}

					if iov.FlagsLazy() {
						pending_writes.Add(1)
						go func() {
							err := maps.mapped.WriteBlocks(iov.Vaddr, buffer)
							if err != nil {
								panic(err)
							}
							pending_writes.Done()
						}()
					} else {
						// Write it to the static in mem store instead...
						static_page_data.AddPageData(iov, buffer)
					}
				}
			}
			go func() {
				ps.Handle(con)
				// All done...
			}()
		}
	}()

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
		pro := protocol.NewProtocolRW(context.TODO(), []io.Reader{con}, []io.Writer{con}, nil)
		go func() {
			_ = pro.Handle()
		}()

		// Lets go through each of the things we want to migrate...
		ctime := time.Now()

		var wg sync.WaitGroup

		for i, s := range src_storage {
			wg.Add(1)
			go func(index int, src *storageInfo) {
				err := migrateDevice(uint32(index), src.name, pro, src)
				if err != nil {
					fmt.Printf("There was an issue migrating the storage %d %v\n", index, err)
				}
				wg.Done()
			}(i, s)
		}
		wg.Wait()

		if serveProgress != nil {
			serveProgress.Wait()
		}
		fmt.Printf("\n\nMigration completed in %dms\n", time.Since(ctime).Milliseconds())

		con.Close()
	}
	shutdown_everything()
}

func shutdown_everything() {
	// first unlock everything
	fmt.Printf("Unlocking devices...\n")
	for _, i := range src_storage {
		i.lockable.Unlock()
		i.tracker.Close()
	}

	fmt.Printf("Shutting down devices cleanly...\n")
	for _, p := range src_exposed {
		device := p.Device()

		fmt.Printf("Shutdown nbd device %s\n", device)
		_ = p.Shutdown()
	}
}

func setupStorageDevice(conf *config.DeviceSchema) (*storageInfo, error) {
	block_size := 1024 * 128

	num_blocks := (int(conf.ByteSize()) + block_size - 1) / block_size

	source, ex, err := device.NewDevice(conf)
	if err != nil {
		return nil, err
	}
	if ex != nil {
		fmt.Printf("Device %s exposed as %s\n", conf.Name, ex.Device())
		src_exposed = append(src_exposed, ex)
	}
	sourceMetrics := modules.NewMetrics(source)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceMetrics, block_size)
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

	schema := string(conf.Encode())

	sinfo := &storageInfo{
		tracker:    sourceDirtyRemote,
		lockable:   sourceStorage,
		orderer:    orderer,
		block_size: block_size,
		num_blocks: num_blocks,
		name:       conf.Name,
		schema:     schema,
		data_complete: func() {
			fmt.Printf("Data %s is complete\n", conf.Name)
		},
	}

	if conf.PageServerPID != 0 {
		// This is going to be used for page server stuff...
		sinfo.mapped = modules.NewMappedStorage(sourceStorage, os.Getpagesize())
	}

	return sinfo, nil
}

// Migrate a device
func migrateDevice(dev_id uint32, name string,
	pro protocol.Protocol,
	sinfo *storageInfo) error {
	size := sinfo.lockable.Size()
	dest := protocol.NewToProtocol(uint64(size), dev_id, pro)

	err := dest.SendDevInfo(name, uint32(sinfo.block_size), sinfo.schema)
	if err != nil {
		return err
	}

	statusString := " "

	statusFn := func(s decor.Statistics) string {
		return statusString
	}

	var bar *mpb.Bar
	if serve_progress {
		bar = serveProgress.AddBar(int64(size),
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
	}()

	go func() {
		_ = dest.HandleDontNeedAt(func(offset int64, length int32) {
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
	}()

	conf := migrator.NewMigratorConfig().WithBlockSize(sinfo.block_size)
	conf.Locker_handler = func() {
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPreLock})
		sinfo.lockable.Lock()
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPostLock})
	}
	conf.Unlocker_handler = func() {
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPreUnlock})
		sinfo.lockable.Unlock()
		_ = dest.SendEvent(&packets.Event{Type: packets.EventPostUnlock})
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 1000000,
	}
	conf.Error_handler = func(b *storage.BlockInfo, err error) {
		// For now...
		panic(err)
	}
	conf.Integrity = true

	last_value := uint64(0)
	last_time := time.Now()

	if serve_progress {

		conf.Progress_handler = func(p *migrator.MigrationProgress) {
			v := uint64(p.Ready_blocks) * uint64(sinfo.block_size)
			if v > size {
				v = size
			}
			bar.SetCurrent(int64(v))
			bar.EwmaIncrInt64(int64(v-last_value), time.Since(last_time))
			last_time = time.Now()
			last_value = v
		}
	} else {
		conf.Progress_handler = func(p *migrator.MigrationProgress) {
			fmt.Printf("[%s] Progress Moved: %d/%d %.2f%% Clean: %d/%d %.2f%% InProgress: %d\n",
				name, p.Migrated_blocks, p.Total_blocks, p.Migrated_blocks_perc,
				p.Ready_blocks, p.Total_blocks, p.Ready_blocks_perc,
				p.Active_blocks)
		}
		conf.Error_handler = func(b *storage.BlockInfo, err error) {
			fmt.Printf("[%s] Error for block %d error %v\n", name, b.Block, err)
		}
	}

	mig, err := migrator.NewMigrator(sinfo.tracker, dest, sinfo.orderer, conf)

	if err != nil {
		return err
	}

	migrate_blocks := sinfo.num_blocks

	// If it's a mapped, configure the migrator for that...
	if sinfo.mapped != nil {
		fmt.Printf("Configuring mapped migrator...\n")
		// Route mapped writes over to WriteAtWithMap
		writer := func(data []byte, offset int64, idmap map[uint64]uint64) (int, error) {
			return dest.WriteAtWithMap(data, offset, idmap)
		}
		mig.SetSourceMapped(sinfo.mapped, writer)

		migrate_blocks = int((uint64(sinfo.mapped.Size()) + uint64(conf.Block_size) - 1) / uint64(conf.Block_size))
		// Since we're not migrating all the blocks, we need to monitor the latter blocks in case there's new data there...
		offset := migrate_blocks * conf.Block_size
		length := (sinfo.num_blocks - migrate_blocks) * conf.Block_size
		sinfo.tracker.TrackAt(int64(offset), int64(length))
		fmt.Printf("Migrating %d/%d blocks. Tracking data at offset %d length %d. Size is %d.\n", migrate_blocks, sinfo.num_blocks, offset, length, sinfo.lockable.Size())
	} else {
		fmt.Printf("Migrating %d blocks\n", migrate_blocks)
	}

	// Now do the migration...
	err = mig.Migrate(migrate_blocks)
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

	if sinfo.mapped != nil {
		// Continuous migration until data_complete called...
		ticker := time.NewTicker(100 * time.Millisecond)
		ctx, cancelfn := context.WithCancel(context.TODO())

		sinfo.data_complete = func() {
			fmt.Printf("Cancelling continuous migration.\n")
			cancelfn()
		}

	cmig:
		for {
			select {
			case <-ticker.C:
				blocks := mig.GetLatestDirty()

				if blocks != nil {
					// Optional: Send the list of dirty blocks over...
					err := dest.DirtyList(conf.Block_size, blocks)
					if err != nil {
						return err
					}

					fmt.Printf("[%s] Migrating dirty blocks %d\n", name, len(blocks))
					err = mig.MigrateDirty(blocks)
					if err != nil {
						return err
					}
				} else {
					select {
					case <-ctx.Done():
						fmt.Printf("Continuous migration cancelled.\n")
						break cmig
					default:
						break
					}
					mig.Unlock()
				}
			}
		}

	} else {

		// Optional: Enter a loop looking for more dirty blocks to migrate...
		for {
			blocks := mig.GetLatestDirty() //
			if !serve_continuous && blocks == nil {
				break
			}

			if blocks != nil {
				// Optional: Send the list of dirty blocks over...
				err := dest.DirtyList(conf.Block_size, blocks)
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
	}

	err = mig.WaitForCompletion()
	if err != nil {
		return err
	}

	fmt.Printf("[%s] Migration completed\n", name)

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
	return nil
}
