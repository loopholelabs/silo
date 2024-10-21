package device

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/migrator"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/rs/zerolog/log"
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
	log.Info().Str("schema", string(ds.Encode())).Msg("Setting up NewDevice from schema")

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
		log.Info().Str("schema", string(ds.Encode())).Msg("Setting up CopyOnWrite")

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
		log.Info().Str("schema", string(ds.Encode())).Msg("Setting up BinLog")
		prov, err = modules.NewBinLog(prov, ds.Binlog)
		if err != nil {
			return nil, nil, err
		}
	}

	// Now optionaly expose the device
	// NB You may well need to call ex.SetProvider if you wish to insert other things in the chain.
	var ex storage.ExposedStorage
	if ds.Expose {
		log.Info().Str("schema", string(ds.Encode())).Msg("Setting up Expose device")

		ex = expose.NewExposedStorageNBDNL(prov, 8, 0, prov.Size(), expose.NBD_DEFAULT_BLOCK_SIZE, true)

		err := ex.Init()
		if err != nil {
			prov.Close()
			return nil, nil, err
		}
	}

	// Optionally sync the device to S3
	if ds.Sync != nil {
		log.Info().Str("schema", string(ds.Encode())).Msg("Setting up Sync")

		//			s3dest, err := sources.NewS3StorageDummy(prov.Size(), bs)
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

		ctx, cancelfn := context.WithCancel(context.TODO())

		// Start doing the sync...
		syncer := migrator.NewSyncer(ctx, &migrator.Sync_config{
			Name:               ds.Name,
			Integrity:          false,
			Cancel_writes:      true,
			Dedupe_writes:      true,
			Tracker:            sourceDirtyRemote,
			Lockable:           sourceStorage,
			Destination:        s3dest,
			Orderer:            orderer,
			Dirty_check_period: ds.Sync.Config.CheckPeriod,
			Dirty_block_getter: func() []uint {
				return sourceDirtyRemote.GetDirtyBlocks(
					ds.Sync.Config.MaxAge, ds.Sync.Config.Limit, ds.Sync.Config.BlockShift, ds.Sync.Config.MinChanged)
			},
			Block_size:       bs,
			Progress_handler: func(p *migrator.MigrationProgress) {},
			Error_handler:    func(b *storage.BlockInfo, err error) {},
		})

		// The provider we return should feed into our sync here.
		prov = sourceStorage

		var wg sync.WaitGroup

		// Sync happens here...
		wg.Add(1)
		go func() {
			// Do this in a goroutine, but make sure it's cancelled etc
			status, err := syncer.Sync(false, true)
			log.Info().Str("schema", string(ds.Encode())).Err(err).Any("status", status).Msg("Sync finished")
			wg.Done()
		}()

		// If the storage enters the "migrating_to" state, we should cancel the sync.
		storage.AddEventNotification(prov, "migrating_to", func(event_type storage.EventType, data storage.EventData) storage.EventReturnData {
			log.Info().Str("schema", string(ds.Encode())).Msg("Sync cancelled as storage transitioned to migrating_to")
			cancelfn()
			// WAIT HERE for the sync to finish
			wg.Wait()
			return nil // TODO: Return the sync blocks
		})

	}

	return prov, ex, nil
}
