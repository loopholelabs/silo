package benchmarks

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/loopholelabs/logging"
	ltypes "github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/config"
	"github.com/loopholelabs/silo/pkg/storage/devicegroup"
	"github.com/loopholelabs/silo/pkg/storage/expose"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

const testFileName = "test_file_name"
const testFileNameCache = "test_file_cache"
const testFileNameState = "test_file_state"
const testFileSize = 1024 * 1024 * 1024

type TestConfig struct {
	readOp         bool
	concurrency    int
	blockSize      int
	name           string
	nbd            bool
	nbdConnections int
	cow            bool
	sparsefile     bool
}

func BenchmarkFile(mb *testing.B) {

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
			b.Cleanup(func() {
				os.Remove(testFileName)
				os.Remove(testFileNameCache)
			})
			fileBase, err := sources.NewFileStorageCreate(testFileName, testFileSize)
			assert.NoError(b, err)
			fileBaseMetrics := modules.NewMetrics(fileBase)
			cache, err := sources.NewFileStorageCreate(testFileNameCache, testFileSize)
			cacheMetrics := modules.NewMetrics(cache)
			assert.NoError(b, err)

			var source storage.Provider
			source = fileBaseMetrics
			if conf.cow {
				source = modules.NewCopyOnWrite(fileBaseMetrics, cacheMetrics, conf.blockSize)
			}

			// General metrics from this side...
			smetrics := modules.NewMetrics(source)

			if conf.nbd {
				exp := expose.NewExposedStorageNBDNL(smetrics, &expose.Config{
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
				err = PerformRandomOp(source.Size(), conf.concurrency, conf.blockSize, b.N, func(buffer []byte, offset int64) error {
					if conf.readOp {
						_, err := f.ReadAt(buffer, offset)
						return err
					} else {
						_, err := f.WriteAt(buffer, offset)
						return err
					}
				})
				assert.NoError(b, err)
				err = f.Close() // Make sure the data is written
				assert.NoError(b, err)

			} else {
				b.ResetTimer()
				err = PerformRandomOp(source.Size(), conf.concurrency, conf.blockSize, b.N, func(buffer []byte, offset int64) error {
					if conf.readOp {
						_, err := smetrics.ReadAt(buffer, offset)
						return err
					} else {
						_, err := smetrics.WriteAt(buffer, offset)
						return err
					}
				})
				assert.NoError(b, err)
			}
			b.SetBytes(int64(conf.blockSize))

			// Show some stats....
			smet := smetrics.GetMetrics()
			if conf.readOp {
				fmt.Printf("reads %d ops %d bytes %dms time\n", smet.ReadOps, smet.ReadBytes, time.Duration(smet.ReadTime).Milliseconds())
			} else {
				fmt.Printf("writes %d ops %d bytes %dms time\n", smet.WriteOps, smet.WriteBytes, time.Duration(smet.WriteTime).Milliseconds())
			}
			if conf.cow {
				bmet := fileBaseMetrics.GetMetrics()
				omet := cacheMetrics.GetMetrics()
				if conf.readOp {
					fmt.Printf(" BaseReads %d ops %d bytes %dms time\n", bmet.ReadOps, bmet.ReadBytes, time.Duration(bmet.ReadTime).Milliseconds())
					fmt.Printf(" OverlayReads %d ops %d bytes %dms time\n", omet.ReadOps, omet.ReadBytes, time.Duration(omet.ReadTime).Milliseconds())
				} else {
					fmt.Printf(" BaseReads %d ops %d bytes %dms time\n", bmet.ReadOps, bmet.ReadBytes, time.Duration(bmet.ReadTime).Milliseconds())
					fmt.Printf(" OverlayReads %d ops %d bytes %dms time\n", omet.ReadOps, omet.ReadBytes, time.Duration(omet.ReadTime).Milliseconds())
					fmt.Printf(" BaseWrites %d ops %d bytes %dms time\n", bmet.WriteOps, bmet.WriteBytes, time.Duration(bmet.WriteTime).Milliseconds())
					fmt.Printf(" OverlayWrites %d ops %d bytes %dms time\n", omet.WriteOps, omet.WriteBytes, time.Duration(omet.WriteTime).Milliseconds())
				}
			}
		})
	}
}

func BenchmarkDevice(mb *testing.B) {
	log := logging.New(logging.Zerolog, "test", os.Stderr)
	log.SetLevel(ltypes.TraceLevel)

	for _, conf := range []TestConfig{
		{readOp: true, name: "randread", concurrency: 1, blockSize: 1024 * 1024},
		{readOp: true, name: "randread", concurrency: 100, blockSize: 4 * 1024},
		{readOp: true, name: "randreadSF", concurrency: 100, blockSize: 4 * 1024, sparsefile: true},
		{readOp: true, name: "randread", concurrency: 100, blockSize: 1024 * 1024},
		{readOp: true, name: "randreadSF", concurrency: 100, blockSize: 1024 * 1024, sparsefile: true},
		{readOp: false, name: "randwrite", concurrency: 1, blockSize: 1024 * 1024},
		{readOp: false, name: "randwrite", concurrency: 100, blockSize: 4 * 1024},
		{readOp: false, name: "randwriteSF", concurrency: 100, blockSize: 4 * 1024, sparsefile: true},
		{readOp: false, name: "randwrite", concurrency: 100, blockSize: 1024 * 1024},
		{readOp: false, name: "randwriteSF", concurrency: 100, blockSize: 1024 * 1024, sparsefile: true},
	} {
		mb.Run(fmt.Sprintf("%s-%d-%d", conf.name, conf.concurrency, conf.blockSize), func(b *testing.B) {

			size := 1024 * 1024 * 1024

			deviceSchemas := []*config.DeviceSchema{
				{
					Name:      "disk",
					BlockSize: "1m",
					Size:      fmt.Sprintf("%d", size),
					Expose:    true,
					System:    "file",
					Location:  testFileName,
					ROSource: &config.DeviceSchema{
						Name:      testFileNameState,
						System:    "file",
						Size:      fmt.Sprintf("%d", size),
						BlockSize: "1m",
						Location:  testFileNameCache,
					},
				},
			}

			if conf.sparsefile {
				deviceSchemas[0].System = "sparsefile"
			}

			dg, err := devicegroup.NewFromSchema("test", deviceSchemas, false, log, nil)
			assert.NoError(b, err)

			b.Cleanup(func() {
				err := dg.CloseAll()
				assert.NoError(b, err)
				os.Remove(testFileName)
				os.Remove(testFileNameCache)
				os.Remove(testFileNameState)
			})

			// Now do some r/w benchmarking on the device...
			di := dg.GetDeviceInformationByName("disk")
			assert.NotNil(b, di)

			// Now run the test
			f, err := os.OpenFile(fmt.Sprintf("/dev/%s", di.Exp.Device()), os.O_RDWR, 0777)
			assert.NoError(b, err)

			b.ResetTimer()
			err = PerformRandomOp(uint64(size), conf.concurrency, conf.blockSize, b.N, func(buffer []byte, offset int64) error {

				if conf.readOp {
					_, err := f.ReadAt(buffer, offset)
					return err
				} else {
					_, err := f.WriteAt(buffer, offset)
					return err
				}
			})
			assert.NoError(b, err)
			err = f.Close() // Make sure the data is written
			assert.NoError(b, err)

			b.SetBytes(int64(conf.blockSize))

			// Maybe show some stats here...

		})
	}
}

const existingEbsFile = "/root/.local/state/architect/packages/gha-runner/oci.ext4"

func BenchmarkEBSDevice(mb *testing.B) {
	log := logging.New(logging.Zerolog, "test", os.Stderr)
	// log.SetLevel(ltypes.TraceLevel)

	for _, conf := range []TestConfig{
		{readOp: true, name: "randread", concurrency: 1, blockSize: 1024 * 1024},
		{readOp: true, name: "randread", concurrency: 100, blockSize: 4 * 1024},
		{readOp: true, name: "randreadSF", concurrency: 100, blockSize: 4 * 1024, sparsefile: true},
		{readOp: true, name: "randread", concurrency: 100, blockSize: 1024 * 1024},
		{readOp: true, name: "randreadSF", concurrency: 100, blockSize: 1024 * 1024, sparsefile: true},
		{readOp: false, name: "randwrite", concurrency: 1, blockSize: 1024 * 1024},
		{readOp: false, name: "randwrite", concurrency: 100, blockSize: 4 * 1024},
		{readOp: false, name: "randwriteSF", concurrency: 100, blockSize: 4 * 1024, sparsefile: true},
		{readOp: false, name: "randwrite", concurrency: 100, blockSize: 1024 * 1024},
		{readOp: false, name: "randwriteSF", concurrency: 100, blockSize: 1024 * 1024, sparsefile: true},
	} {
		mb.Run(fmt.Sprintf("%s-%d-%d", conf.name, conf.concurrency, conf.blockSize), func(b *testing.B) {

			size := 58595098624

			deviceSchemas := []*config.DeviceSchema{
				{
					Name:      "disk",
					BlockSize: "1m",
					Size:      fmt.Sprintf("%d", size),
					Expose:    true,
					System:    "file",
					Location:  existingEbsFile,
					ROSource: &config.DeviceSchema{
						Name:      testFileNameState,
						System:    "file",
						Size:      fmt.Sprintf("%d", size),
						BlockSize: "1m",
						Location:  testFileNameCache,
					},
				},
			}

			if conf.sparsefile {
				deviceSchemas[0].System = "sparsefile"
			}

			dg, err := devicegroup.NewFromSchema("test", deviceSchemas, false, log, nil)
			assert.NoError(b, err)

			b.Cleanup(func() {
				err := dg.CloseAll()
				assert.NoError(b, err)
				os.Remove(testFileName)
				os.Remove(testFileNameCache)
				os.Remove(testFileNameState)
			})

			// Now do some r/w benchmarking on the device...
			di := dg.GetDeviceInformationByName("disk")
			assert.NotNil(b, di)

			// Now run the test
			f, err := os.OpenFile(fmt.Sprintf("/dev/%s", di.Exp.Device()), os.O_RDWR, 0777)
			assert.NoError(b, err)

			b.ResetTimer()
			err = PerformRandomOp(uint64(size), conf.concurrency, conf.blockSize, b.N, func(buffer []byte, offset int64) error {

				if conf.readOp {
					_, err := f.ReadAt(buffer, offset)
					return err
				} else {
					_, err := f.WriteAt(buffer, offset)
					return err
				}
			})
			assert.NoError(b, err)
			err = f.Close() // Make sure the data is written
			assert.NoError(b, err)

			b.SetBytes(int64(conf.blockSize))

			// Maybe show some stats here...

		})
	}
}
