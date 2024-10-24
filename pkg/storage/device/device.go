package device

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

const (
	SYSTEM_MEMORY      = "memory"
	SYSTEM_FILE        = "file"
	SYSTEM_SPARSE_FILE = "sparsefile"
	SYSTEM_S3          = "s3"
	DEFAULT_BLOCK_SIZE = 4096
)

type Device struct {
	Provider storage.StorageProvider
	Exposed  storage.ExposedStorage
}

func NewDevices(ds []*config.DeviceSchema) (map[string]*Device, error) {
	devices := make(map[string]*Device)
	for _, c := range ds {
		dev, ex, err := NewDevice(c)
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

func NewDevice(ds *config.DeviceSchema) (storage.StorageProvider, storage.ExposedStorage, error) {

	var prov storage.StorageProvider
	var err error

	bs := int(ds.ByteBlockSize())
	if bs == 0 {
		bs = DEFAULT_BLOCK_SIZE
	}

	if ds.System == SYSTEM_MEMORY {
		// Create some memory storage...
		cr := func(i int, s int) (storage.StorageProvider, error) {
			return sources.NewMemoryStorage(s), nil
		}
		// Setup some sharded memory storage (for concurrent write speed)
		prov, err = modules.NewShardedStorage(int(ds.ByteSize()), bs, cr)
		if err != nil {
			return nil, nil, err
		}
	} else if ds.System == SYSTEM_S3 {
		//
		return nil, nil, fmt.Errorf("S3 Not Supported in device yet")
	} else if ds.System == SYSTEM_SPARSE_FILE {
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
	} else if ds.System == SYSTEM_FILE {

		// Check what we have been given...
		file, err := os.Open(ds.Location)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// It doesn't exist, so lets create it and return
				prov, err = sources.NewFileStorageCreate(ds.Location, int64(ds.ByteSize()))
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
				prov, err = sources.NewFileStorage(ds.Location, int64(ds.ByteSize()))
				if err != nil {
					return nil, nil, err
				}
			} else {

				// file is a directory, lets use it for shards

				cr := func(i int, s int) (storage.StorageProvider, error) {
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
	} else {
		return nil, nil, fmt.Errorf("unsupported storage system %s", ds.System)

	}

	// Optionally use a copy on write RO source...
	if ds.ROSource != nil {

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
		} else if errors.Is(err, os.ErrNotExist) {
			// Doesn't exists, so it's a new cow
		} else {
			return nil, nil, err
		}

		// Make sure the cow data gets dumped on close...
		cow.Close_fn = func() {
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
		prov, err = modules.NewBinLog(prov, ds.Binlog)
		if err != nil {
			return nil, nil, err
		}
	}

	// Now optionaly expose the device
	// NB You may well need to call ex.SetProvider if you wish to insert other things in the chain.
	var ex storage.ExposedStorage
	if ds.Expose {

		ex = expose.NewExposedStorageNBDNL(prov, 8, 0, prov.Size(), expose.NBD_DEFAULT_BLOCK_SIZE, true)

		err := ex.Init()
		if err != nil {
			prov.Close()
			return nil, nil, err
		}
	}

	// Optionally sync the device to S3
	if ds.Sync != nil {

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

		dirty_block_size := bs >> ds.Sync.Config.BlockShift

		num_blocks := (int(prov.Size()) + bs - 1) / bs

		sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(prov, dirty_block_size)
		sourceStorage := modules.NewLockable(sourceDirtyLocal)

		// Setup a block order
		orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
		orderer.AddAll()

		check_period, err := time.ParseDuration(ds.Sync.Config.CheckPeriod)
		if err != nil {
			prov.Close()
			return nil, nil, err
		}

		max_age, err := time.ParseDuration(ds.Sync.Config.MaxAge)
		if err != nil {
			prov.Close()
			return nil, nil, err
		}

		ctx, cancelfn := context.WithCancel(context.TODO())

		// Start doing the sync...
		syncer := migrator.NewSyncer(ctx, &migrator.SyncConfig{
			Name:             ds.Name,
			Integrity:        false,
			CancelWrites:     true,
			DedupeWrites:     true,
			Tracker:          sourceDirtyRemote,
			Lockable:         sourceStorage,
			Destination:      s3dest,
			Orderer:          orderer,
			DirtyCheckPeriod: check_period,
			DirtyBlockGetter: func() []uint {
				return sourceDirtyRemote.GetDirtyBlocks(
					max_age, ds.Sync.Config.Limit, ds.Sync.Config.BlockShift, ds.Sync.Config.MinChanged)
			},
			BlockSize:       bs,
			ProgressHandler: func(p *migrator.MigrationProgress) {},
			ErrorHandler:    func(b *storage.BlockInfo, err error) {},
		})

		// The provider we return should feed into our sync here.
		prov = sourceStorage

		var sync_lock sync.Mutex
		var sync_running bool
		var wg sync.WaitGroup

		// If the storage gets a "sync.start", we should start syncing to S3.
		storage.AddEventNotification(prov, "sync.start", func(event_type storage.EventType, data storage.EventData) storage.EventReturnData {
			sync_lock.Lock()
			if sync_running {
				sync_lock.Unlock()
				return false
			}
			sync_running = true
			wg.Add(1)
			sync_lock.Unlock()

			// Sync happens here...
			go func() {
				// Do this in a goroutine, but make sure it's cancelled etc
				_, _ = syncer.Sync(false, true)
				wg.Done()
			}()
			return true
		})

		// If the storage gets a "sync.status", get some status on the S3Storage
		storage.AddEventNotification(prov, "sync.status", func(event_type storage.EventType, data storage.EventData) storage.EventReturnData {
			return s3dest.Metrics()
		})

		// If the storage gets a "sync.stop", we should cancel the sync, and return the safe blocks
		storage.AddEventNotification(prov, "sync.stop", func(event_type storage.EventType, data storage.EventData) storage.EventReturnData {
			sync_lock.Lock()
			if !sync_running {
				sync_lock.Unlock()
				return nil
			}
			cancelfn()
			// WAIT HERE for the sync to finish
			wg.Wait()
			sync_running = false
			sync_lock.Unlock()

			// Get the list of safe blocks we can use.
			blocks := syncer.GetSafeBlockMap()
			// Translate these to locations so they can be sent to a destination...
			alt_sources := make([]packets.AlternateSource, 0)
			for block, hash := range blocks {
				as := packets.AlternateSource{
					Offset:   int64(block * uint(bs)),
					Length:   int64(bs),
					Hash:     hash,
					Location: fmt.Sprintf("%s %s %s", ds.Sync.Endpoint, ds.Sync.Bucket, ds.Name),
				}
				alt_sources = append(alt_sources, as)
			}

			return alt_sources
		})

	}

	return prov, ex, nil
}
