package device

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/metrics"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
)

const (
	SystemMemory     = "memory"
	SystemFile       = "file"
	SystemSparseFile = "sparsefile"
	SystemS3         = "s3"
	DefaultBlockSize = 4096
)

var syncConcurrency = map[int]int{storage.BlockTypeAny: 10}
var syncGrabConcurrency = 100
var syncVolatilityExpiry = 10 * time.Minute

type Device struct {
	Provider storage.Provider
	Exposed  storage.ExposedStorage
}

func NewDevices(ds []*config.DeviceSchema) (map[string]*Device, error) {
	return NewDevicesWithLogging(ds, nil)
}

func NewDevicesWithLogging(ds []*config.DeviceSchema, log types.Logger) (map[string]*Device, error) {
	devices := make(map[string]*Device)
	for _, c := range ds {
		dev, ex, err := NewDeviceWithLogging(c, log)
		if err != nil {
			// Close/shutdown any we already setup, but we'll ignore any close errors here.
			for _, cc := range devices {
				cc.Provider.Close()
				if cc.Exposed != nil {
					_ = cc.Exposed.Shutdown()
				}
			}
			return nil, err
		}
		devices[c.Name] = &Device{
			Provider: dev,
			Exposed:  ex,
		}
	}
	return devices, nil
}

func NewDevice(ds *config.DeviceSchema) (storage.Provider, storage.ExposedStorage, error) {
	return NewDeviceWithLogging(ds, nil)
}

func NewDeviceWithLogging(ds *config.DeviceSchema, log types.Logger) (storage.Provider, storage.ExposedStorage, error) {
	return NewDeviceWithLoggingMetrics(ds, log, nil)
}

func NewDeviceWithLoggingMetrics(ds *config.DeviceSchema, log types.Logger, met metrics.SiloMetrics) (storage.Provider, storage.ExposedStorage, error) {

	if log != nil {
		log.Debug().Str("name", ds.Name).Msg("creating new device")
	}

	var prov storage.Provider
	var err error

	bs := int(ds.ByteBlockSize())
	if bs == 0 {
		bs = DefaultBlockSize
	}

	switch ds.System {

	case SystemMemory:
		// Create some memory storage...
		cr := func(_ int, s int) (storage.Provider, error) {
			return sources.NewMemoryStorage(s), nil
		}
		// Setup some sharded memory storage (for concurrent write speed)
		prov, err = modules.NewShardedStorage(int(ds.ByteSize()), bs, cr)
		if err != nil {
			return nil, nil, err
		}
	case SystemS3:
		//
		return nil, nil, fmt.Errorf("S3 Not Supported in device yet")
	case SystemSparseFile:
		file, err := os.Open(ds.Location)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// It doesn't exist, so lets create it and return
				prov, err = sources.NewFileStorageSparseCreate(ds.Location, uint64(ds.ByteSize()), bs)
				if err != nil {
					return nil, nil, err
				}
			} else {
				return nil, nil, err
			}
		} else {
			file.Close()
			prov, err = sources.NewFileStorageSparse(ds.Location, uint64(ds.ByteSize()), bs)
			if err != nil {
				return nil, nil, err
			}
		}
	case SystemFile:

		// Check what we have been given...
		file, err := os.Open(ds.Location)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// It doesn't exist, so lets create it and return
				prov, err = sources.NewFileStorageCreate(ds.Location, ds.ByteSize())
				if err != nil {
					return nil, nil, err
				}
			} else {
				return nil, nil, err
			}
		} else {
			defer file.Close()

			fileInfo, err := file.Stat()
			if err != nil {
				return nil, nil, err
			}

			// IsDir is short for fileInfo.Mode().IsDir()
			if !fileInfo.IsDir() {
				// file is a file, use it as is
				prov, err = sources.NewFileStorage(ds.Location, ds.ByteSize())
				if err != nil {
					return nil, nil, err
				}
			} else {

				// file is a directory, lets use it for shards

				cr := func(i int, s int) (storage.Provider, error) {
					// Check if the file exists, and is the correct size. If not, create it.
					f := path.Join(ds.Location, fmt.Sprintf("file_%d", i))
					file, err := os.Open(f)
					if errors.Is(err, os.ErrNotExist) {
						prov, err := sources.NewFileStorageCreate(f, int64(s))
						if err != nil {
							return nil, err
						}
						return prov, nil
					}
					if err != nil {
						return nil, err
					}
					defer file.Close()

					fileinfo, err := file.Stat()
					if err != nil {
						return nil, err
					}
					if fileinfo.Size() != int64(s) {
						return nil, fmt.Errorf("file exists but incorrect size")
					}

					return sources.NewFileStorage(f, int64(s))
				}
				// Setup some sharded memory storage (for concurrent write speed)
				prov, err = modules.NewShardedStorage(int(ds.ByteSize()), bs, cr)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	default:
		return nil, nil, fmt.Errorf("unsupported storage system %s", ds.System)

	}

	// Optionally use a copy on write RO source...
	if ds.ROSource != nil {
		if log != nil {
			log.Debug().Str("name", ds.Name).Msg("setting up CopyOnWrite")
		}

		// Create the ROSource...
		rodev, _, err := NewDevice(ds.ROSource)
		if err != nil {
			return nil, nil, err
		}

		// Now hook it in as the read only source for this device...
		cow := modules.NewCopyOnWrite(rodev, prov, bs)
		prov = cow
		// If we can find a cow file, load it up...
		data, err := os.ReadFile(ds.ROSource.Name)
		if err == nil {
			// Load up the blocks...
			blocks := make([]uint, 0)
			for i := 0; i < len(data); i += 4 {
				v := binary.LittleEndian.Uint32(data[i:])
				blocks = append(blocks, uint(v))
			}
			cow.SetBlockExists(blocks)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, err
		}

		// Make sure the cow data gets dumped on close...
		cow.CloseFn = func() {
			if log != nil {
				log.Debug().Str("name", ds.Name).Msg("Writing CopyOnWrite state")
			}

			blocks := cow.GetBlockExists()
			// Write it out to file
			data := make([]byte, 0)
			for _, b := range blocks {
				data = binary.LittleEndian.AppendUint32(data, uint32(b))
			}
			err := os.WriteFile(ds.ROSource.Name, data, 0666)
			if err != nil {
				panic(fmt.Sprintf("COW write state failed with %v", err))
			}
		}
	}

	// Optionally binlog this dev to a file
	if ds.Binlog != "" {
		if log != nil {
			log.Debug().Str("name", ds.Name).Msg("logging to binlog")
		}

		prov, err = modules.NewBinLog(prov, ds.Binlog)
		if err != nil {
			return nil, nil, err
		}
	}

	if met != nil {
		// Expose some basic metrics for the devices storage.
		metrics := modules.NewMetrics(prov)
		met.AddMetrics(fmt.Sprintf("device_%s", ds.Name), metrics)
		prov = metrics
	}

	// Now optionaly expose the device
	// NB You may well need to call ex.SetProvider if you wish to insert other things in the chain.
	var ex storage.ExposedStorage
	if ds.Expose {
		nbdex := expose.NewExposedStorageNBDNL(prov, expose.DefaultConfig.WithLogger(log))
		ex = nbdex

		err := ex.Init()
		if err != nil {
			prov.Close()
			return nil, nil, err
		}
		if log != nil {
			log.Debug().Str("name", ds.Name).Str("device", ex.Device()).Msg("device exposed as nbd device")
		}

		if met != nil {
			met.AddNBD(ds.Name, nbdex)
		}
	}

	// Optionally sync the device to S3
	if ds.Sync != nil {
		if log != nil {
			log.Debug().Str("name", ds.Name).Msg("setting up S3 sync")
		}

		s3dest, err := sources.NewS3StorageCreate(ds.Sync.Secure,
			ds.Sync.Endpoint,
			ds.Sync.AccessKey,
			ds.Sync.SecretKey,
			ds.Sync.Bucket,
			ds.Name,
			prov.Size(),
			bs)

		if err != nil {
			prov.Close()
			return nil, nil, err
		}

		if met != nil {
			met.AddS3Storage(fmt.Sprintf("s3sync_%s", ds.Name), s3dest)
		}

		dirtyBlockSize := bs >> ds.Sync.Config.BlockShift

		// numBlocks := (int(prov.Size()) + bs - 1) / bs

		vm := volatilitymonitor.NewVolatilityMonitor(prov, bs, syncVolatilityExpiry)

		sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(vm, dirtyBlockSize)
		sourceStorage := modules.NewLockable(sourceDirtyLocal)

		if met != nil {
			met.AddDirtyTracker(fmt.Sprintf("s3sync_%s", ds.Name), sourceDirtyRemote)
		}

		// Setup a block order
		orderer := vm
		// orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
		orderer.AddAll()

		checkPeriod, err := time.ParseDuration(ds.Sync.Config.CheckPeriod)
		if err != nil {
			prov.Close()
			return nil, nil, err
		}

		maxAge, err := time.ParseDuration(ds.Sync.Config.MaxAge)
		if err != nil {
			prov.Close()
			return nil, nil, err
		}

		ctx, cancelfn := context.WithCancel(context.TODO())

		// Start doing the sync...
		syncer := migrator.NewSyncer(ctx, &migrator.SyncConfig{
			Concurrency:      syncConcurrency,
			Logger:           log,
			Name:             ds.Name,
			Integrity:        false,
			CancelWrites:     true,
			DedupeWrites:     true,
			Tracker:          sourceDirtyRemote,
			Lockable:         sourceStorage,
			Destination:      s3dest,
			Orderer:          orderer,
			DirtyCheckPeriod: checkPeriod,
			DirtyBlockGetter: func() []uint {
				return sourceDirtyRemote.GetDirtyBlocks(
					maxAge, ds.Sync.Config.Limit, ds.Sync.Config.BlockShift, ds.Sync.Config.MinChanged)
			},
			BlockSize:       bs,
			ProgressHandler: func(_ *migrator.MigrationProgress) {},
			ErrorHandler:    func(_ *storage.BlockInfo, _ error) {},
		})

		if met != nil {
			met.AddSyncer(fmt.Sprintf("s3sync_%s", ds.Name), syncer)
		}

		// The provider we return should feed into our sync here...
		prov = sourceStorage

		var syncLock sync.Mutex
		var syncRunning bool
		var wg sync.WaitGroup

		startSync := func(_ storage.EventType, data storage.EventData) storage.EventReturnData {
			if log != nil {
				log.Debug().Str("name", ds.Name).Msg("sync.start called")
			}
			if data != nil {
				startConfig := data.(storage.SyncStartConfig)

				var wg sync.WaitGroup

				concurrency := make(chan bool, syncGrabConcurrency)

				// Pull these blocks in parallel
				for _, as := range startConfig.AlternateSources {
					wg.Add(1)
					concurrency <- true
					go func(a packets.AlternateSource) {
						buffer := make([]byte, a.Length)
						n, err := s3dest.ReadAt(buffer, a.Offset)
						if err != nil || n != int(a.Length) {
							panic(fmt.Sprintf("sync.start unable to read from S3. %v", err))
						}

						// Check the data in S3 hasn't changed.
						hash := sha256.Sum256(buffer)
						if !bytes.Equal(hash[:], a.Hash[:]) {
							panic("The data in S3 is corrupt.")
						}

						n, err = startConfig.Destination.WriteAt(buffer, a.Offset)
						if err != nil || n != int(a.Length) {
							panic(fmt.Sprintf("sync.start unable to write data to device from S3. %v", err))
						}
						<-concurrency
						wg.Done()
					}(as)
				}
				wg.Wait() // Wait for all S3 requests to complete
			}

			syncLock.Lock()
			if syncRunning {
				syncLock.Unlock()
				return false
			}
			syncRunning = true
			wg.Add(1)
			syncLock.Unlock()

			// Sync happens here...
			go func() {
				// Do this in a goroutine. It'll get cancelled via context
				_, _ = syncer.Sync(!ds.Sync.Config.OnlyDirty, true)
				wg.Done()
			}()
			return true
		}

		stopSyncing := func(cancelWrites bool) storage.EventReturnData {
			if log != nil {
				log.Debug().Str("name", ds.Name).Msg("sync.stop called")
			}
			syncLock.Lock()
			if !syncRunning {
				syncLock.Unlock()
				return nil
			}
			cancelfn()

			if cancelWrites {
				s3dest.CancelWrites(0, int64(s3dest.Size()))
			}

			// WAIT HERE for the sync to finish
			wg.Wait()
			syncRunning = false
			syncLock.Unlock()

			// Get the list of safe blocks we can use.
			blocks := syncer.GetSafeBlockMap()
			// Translate these to locations so they can be sent to a destination...
			altSources := make([]packets.AlternateSource, 0)
			for block, hash := range blocks {
				as := packets.AlternateSource{
					Offset:   int64(block * uint(bs)),
					Length:   int64(bs),
					Hash:     hash,
					Location: fmt.Sprintf("%s %s %s", ds.Sync.Endpoint, ds.Sync.Bucket, ds.Name),
				}
				altSources = append(altSources, as)
			}

			if log != nil {
				log.Debug().Str("name", ds.Name).Int("sources", len(altSources)).Msg("sync.stop returning altSources")
			}

			return altSources
		}

		stopSync := func(_ storage.EventType, _ storage.EventData) storage.EventReturnData {
			return stopSyncing(false)
		}

		// If the storage gets a "sync.stop", we should cancel the sync, and return the safe blocks
		storage.AddSiloEventNotification(prov, "sync.stop", stopSync)

		// If the storage gets a "sync.start", we should start syncing to S3.
		storage.AddSiloEventNotification(prov, "sync.start", startSync)

		// If the storage gets a "sync.status", get some status on the S3Storage
		storage.AddSiloEventNotification(prov, "sync.status", func(_ storage.EventType, _ storage.EventData) storage.EventReturnData {
			return s3dest.Metrics()
		})

		// If the storage gets a "sync.running", return
		storage.AddSiloEventNotification(prov, "sync.running", func(_ storage.EventType, _ storage.EventData) storage.EventReturnData {
			syncLock.Lock()
			defer syncLock.Unlock()
			return syncRunning
		})

		if ds.Sync.AutoStart {
			// Start the sync here...
			startSync("sync.start", nil)
		}

		hooks := modules.NewHooks(prov)
		hooks.PostClose = func(err error) error {
			// We should stop any sync here, but ask it to cancel any existing writes if possible.
			stopSyncing(true)
			return err
		}
		prov = hooks
	}

	return prov, ex, nil
}
