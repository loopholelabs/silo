package benchmarks

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

const testFileName = "test_file_name"
const testFileNameCache = "test_file_cache"
const testFileSize = 1024 * 1024 * 1024

type TestConfig struct {
	readOp         bool
	concurrency    int
	blockSize      int
	name           string
	nbd            bool
	nbdConnections int
	cow            bool
}

func BenchmarkFile(mb *testing.B) {
	mb.Cleanup(func() {
		os.Remove(testFileName)
		os.Remove(testFileNameCache)
	})
	fileBase, err := sources.NewFileStorageCreate(testFileName, testFileSize)
	assert.NoError(mb, err)
	cache, err := sources.NewFileStorageCreate(testFileNameCache, testFileSize)
	assert.NoError(mb, err)

	for _, conf := range []TestConfig{
		{readOp: true, name: "randread", concurrency: 1, blockSize: 1024 * 1024},
		{readOp: true, name: "randread", concurrency: 100, blockSize: 1024 * 1024},
		{readOp: true, name: "CowRandread", concurrency: 100, blockSize: 1024 * 1024, cow: true},
		{readOp: true, name: "randreadNBD", concurrency: 1, blockSize: 1024 * 1024, nbd: true, nbdConnections: 8},
		{readOp: true, name: "randreadNBD", concurrency: 100, blockSize: 1024 * 1024, nbd: true, nbdConnections: 8},
		{readOp: true, name: "CowRandreadNBD", concurrency: 100, blockSize: 4 * 1024, nbd: true, nbdConnections: 8, cow: true},
		{readOp: true, name: "CowRandreadNBD", concurrency: 100, blockSize: 1024 * 1024, nbd: true, nbdConnections: 8, cow: true},

		{readOp: false, name: "randwrite", concurrency: 1, blockSize: 1024 * 1024},
		{readOp: false, name: "randwrite", concurrency: 100, blockSize: 1024 * 1024},
		{readOp: false, name: "CowRandwrite", concurrency: 100, blockSize: 1024 * 1024, cow: true},
		{readOp: false, name: "randwriteNBD", concurrency: 1, blockSize: 1024 * 1024, nbd: true, nbdConnections: 8},
		{readOp: false, name: "randwriteNBD", concurrency: 100, blockSize: 1024 * 1024, nbd: true, nbdConnections: 8},
		{readOp: false, name: "CowRandwriteNBD", concurrency: 100, blockSize: 1024 * 1024, nbd: true, nbdConnections: 8, cow: true},
		{readOp: false, name: "CowRandwriteNBD", concurrency: 100, blockSize: 4 * 1024, nbd: true, nbdConnections: 8, cow: true},
	} {
		mb.Run(fmt.Sprintf("%s-%d-%d", conf.name, conf.concurrency, conf.blockSize), func(b *testing.B) {
			var source storage.Provider
			source = fileBase
			if conf.cow {
				source = modules.NewCopyOnWrite(fileBase, cache, conf.blockSize)
			}

			if conf.nbd {
				exp := expose.NewExposedStorageNBDNL(source, &expose.Config{
					Logger:         nil,
					NumConnections: conf.nbdConnections,
					Timeout:        30 * time.Second,
					AsyncReads:     true,
					AsyncWrites:    true,
					BlockSize:      expose.DefaultConfig.BlockSize,
				})

				err = exp.Init()
				assert.NoError(b, err)

				b.Cleanup(func() {
					err = exp.Shutdown()
					assert.NoError(b, err)
				})

				f, err := os.OpenFile(fmt.Sprintf("/dev/%s", exp.Device()), os.O_RDWR, 0777)
				assert.NoError(b, err)

				b.ResetTimer()
				PerformRandomOp(source.Size(), conf.concurrency, conf.blockSize, b.N, func(buffer []byte, offset int64) error {
					if conf.readOp {
						_, err := f.ReadAt(buffer, offset)
						return err
					} else {
						_, err := f.WriteAt(buffer, offset)
						return err
					}
				})
				err = f.Close() // Make sure the data is written
				assert.NoError(b, err)

			} else {
				PerformRandomOp(source.Size(), conf.concurrency, conf.blockSize, b.N, func(buffer []byte, offset int64) error {
					if conf.readOp {
						_, err := source.ReadAt(buffer, offset)
						return err
					} else {
						_, err := source.WriteAt(buffer, offset)
						return err
					}
				})
			}
			b.SetBytes(int64(conf.blockSize))
		})
	}
}
