package modules

import (
	"fmt"
	"os"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

const (
	SYSTEM_MEMORY = "memory"
	SYSTEM_FILE   = "file"
)

func New(ds *config.DeviceSchema) (storage.StorageProvider, error) {
	if ds.System == SYSTEM_MEMORY {
		// Create some memory storage...
		cr := func(i int, s int) storage.StorageProvider {
			return sources.NewMemoryStorage(s)
		}
		// Setup some sharded memory storage (for concurrent write speed)
		shardSize := ds.ByteSize()
		if ds.ByteSize() > 64*1024 {
			shardSize = ds.ByteSize() / 1024
		}
		storage := NewShardedStorage(int(ds.ByteSize()), int(shardSize), cr)
		return storage, nil
	} else if ds.System == SYSTEM_FILE {

		// Check what we have been given...
		file, err := os.Open(ds.Location)
		if err != nil {
			if err == os.ErrNotExist {
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
		/*
			cr := func(s int) storage.StorageProvider {

				return NewFileStorageCreate(s)
			}
			// Setup some sharded memory storage (for concurrent write speed)
			shardSize := ds.ByteSize()
			if ds.ByteSize() > 64*1024 {
				shardSize = ds.ByteSize() / 1024
			}
			storage := modules.NewShardedStorage(int(ds.ByteSize()), int(shardSize), cr)
			return storage, nil
		*/
	}
	return nil, fmt.Errorf("Unsupported storage system %s", ds.System)
}
