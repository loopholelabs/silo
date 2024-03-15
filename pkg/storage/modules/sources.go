package modules

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

const (
	SYSTEM_MEMORY      = "memory"
	SYSTEM_FILE        = "file"
	DEFAULT_BLOCK_SIZE = 4096
)

func NewDevices(ds []*config.DeviceSchema) (map[string]storage.StorageProvider, error) {
	devices := make(map[string]storage.StorageProvider)
	for _, c := range ds {
		dev, err := NewDevice(c)
		if err != nil {
			// Close/shutdown any we already setup, but we'll ignore any close errors here.
			for _, cc := range devices {
				cc.Close()
			}
			return nil, err
		}
		devices[c.Name] = dev
	}
	return devices, nil
}

func NewDevice(ds *config.DeviceSchema) (storage.StorageProvider, error) {
	bs := ds.BlockSize
	if bs == 0 {
		bs = DEFAULT_BLOCK_SIZE
	}

	if ds.System == SYSTEM_MEMORY {
		// Create some memory storage...
		cr := func(i int, s int) (storage.StorageProvider, error) {
			return sources.NewMemoryStorage(s), nil
		}
		// Setup some sharded memory storage (for concurrent write speed)
		return NewShardedStorage(int(ds.ByteSize()), bs, cr)
	} else if ds.System == SYSTEM_FILE {

		// Check what we have been given...
		file, err := os.Open(ds.Location)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// It doesn't exist, so lets create it and return
				return sources.NewFileStorageCreate(ds.Location, int64(ds.ByteSize()))
			}
			return nil, err
		}
		defer file.Close()

		fileInfo, err := file.Stat()
		if err != nil {
			return nil, err
		}

		// IsDir is short for fileInfo.Mode().IsDir()
		if !fileInfo.IsDir() {
			// file is a file, use it as is
			return sources.NewFileStorage(ds.Location, int64(ds.ByteSize()))
		}

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
			if fileinfo.Size() != int64(s) {
				return nil, fmt.Errorf("File exists but incorrect size")
			}

			return sources.NewFileStorage(f, int64(s))
		}
		// Setup some sharded memory storage (for concurrent write speed)
		return NewShardedStorage(int(ds.ByteSize()), bs, cr)
	}
	return nil, fmt.Errorf("Unsupported storage system %s", ds.System)
}
