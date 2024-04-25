package device

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/modules"
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
		return nil, nil, fmt.Errorf("S3 Not Supported yet")
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

	// Now optionaly expose the device
	var ex storage.ExposedStorage
	if ds.Expose {
		ex = expose.NewExposedStorageNBDNL(prov, 8, 0, prov.Size(), expose.NBD_DEFAULT_BLOCK_SIZE, true)

		err := ex.Init()
		if err != nil {
			prov.Close()
			return nil, nil, err
		}
	}

	return prov, ex, nil
}
