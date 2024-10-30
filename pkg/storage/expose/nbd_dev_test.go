package expose

import (
	"fmt"
	"math/rand"
	"os"
	"os/user"
	"sync"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

func BenchmarkDevReadNL(mb *testing.B) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	cons := []int{1, 4, 16, 32}
	sizes := []int64{4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024}
	diskSize := 1024 * 1024 * 1024 * 4

	for _, c := range cons {
		for _, v := range sizes {
			name := fmt.Sprintf("readsize_%d_cons_%d", v, c)
			mb.Run(name, func(b *testing.B) {

				store := sources.NewMemoryStorage(diskSize)
				driver := modules.NewMetrics(store)

				n := NewExposedStorageNBDNL(driver, c, 0, driver.Size(), 4096, true)

				err := n.Init()
				if err != nil {
					panic(err)
				}

				/**
				* Cleanup everything
				*
				 */
				b.Cleanup(func() {
					err := n.Shutdown()
					if err != nil {
						fmt.Printf("Error cleaning up %v\n", err)
					}
				})

				//				driver.ResetMetrics() // Only start counting from now...

				// Here's the actual benchmark...

				numReaders := 1

				devfiles := make([]*os.File, 0)

				for i := 0; i < numReaders; i++ {
					df, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", n.deviceIndex), os.O_RDWR, 0666)
					if err != nil {
						panic("Error opening dev file\n")
					}

					devfiles = append(devfiles, df)
				}

				b.Cleanup(func() {
					for _, df := range devfiles {
						df.Close()
					}
				})

				var wg sync.WaitGroup

				readSize := v

				offsets := make([]int, b.N)
				buffers := make(chan []byte, 100)

				for i := 0; i < 100; i++ {
					buffer := make([]byte, readSize)
					buffers <- buffer
				}

				for i := 0; i < b.N; i++ {
					offsets[i] = (rand.Intn(diskSize-int(readSize)) / 4096) * 4096
				}

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					wg.Add(1)

					// Test read speed...
					go func(buff []byte, off int) {
						dfi := rand.Intn(len(devfiles))
						n, err := devfiles[dfi].ReadAt(buff, int64(off))
						if n != len(buff) || err != nil {
							fmt.Printf("Error reading file %d %v\n", n, err)
						}

						wg.Done()
						buffers <- buff
					}(<-buffers, offsets[i])
				}

				b.SetBytes(readSize)

				wg.Wait()

				//				duration := time.Since(ctime).Seconds()
				//				mb_per_sec := float64(totalData) / (duration * 1024 * 1024)

				//				fmt.Printf("Total Data %d from %d ops in %dms RATE %.2fMB/s\n", totalData, b.N, time.Since(ctime).Milliseconds(), mb_per_sec)
				//				driver.ShowStats("stats")
			})
		}
	}
}

func BenchmarkDevReadNLLatency(mb *testing.B) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	latencies := []time.Duration{0, 1 * time.Millisecond, 5 * time.Millisecond}
	cons := []int{1, 16}
	asyncs := []bool{false, true}
	diskSize := 1024 * 1024 * 1024 * 8

	for _, lts := range latencies {
		for _, asy := range asyncs {
			for _, cns := range cons {
				name := fmt.Sprintf("latency_%dms_cons_%d_async_%t", lts.Milliseconds(), cns, asy)
				mb.Run(name, func(b *testing.B) {

					store := sources.NewMemoryStorage(diskSize)
					lstore := modules.NewArtificialLatency(store, lts, 0, lts, 0)
					//					logstore := modules.NewLogger(lstore)
					driver := modules.NewMetrics(lstore)

					data := make([]byte, 4096)
					for p := 0; p < len(data); p++ {
						data[p] = 0x9a
					}
					for off := 0; off < diskSize; off += 4096 {
						n, err := store.WriteAt(data, int64(off))
						if n != len(data) || err != nil {
							panic(fmt.Sprintf("Can't WriteAt %v\n", err))
						}
					}

					n := NewExposedStorageNBDNL(driver, cns, 0, driver.Size(), 4096, asy)

					err := n.Init()
					if err != nil {
						panic(err)
					}

					/**
					* Cleanup everything
					*
					 */
					b.Cleanup(func() {
						err := n.Shutdown()
						if err != nil {
							fmt.Printf("Error cleaning up %v\n", err)
						}
						//						driver.ShowStats(fmt.Sprintf("Metrics-%d", b.N))
					})

					//				driver.ResetMetrics() // Only start counting from now...

					// Here's the actual benchmark...

					numReaders := 8

					devfiles := make([]*os.File, 0)

					for i := 0; i < numReaders; i++ {
						df, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", n.deviceIndex), os.O_RDWR, 0666)
						if err != nil {
							panic("Error opening dev file\n")
						}

						devfiles = append(devfiles, df)
					}

					b.Cleanup(func() {
						for _, df := range devfiles {
							df.Close()
						}
					})

					var wg sync.WaitGroup

					readSize := int64(65536)

					offsets := make([]int, b.N)
					buffers := make(chan []byte, 100)

					for i := 0; i < 100; i++ {
						buffer := make([]byte, readSize)
						buffers <- buffer
					}

					for i := 0; i < b.N; i++ {
						offsets[i] = (rand.Intn(diskSize-int(readSize)) / 4096) * 4096
					}

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						wg.Add(1)

						// Test read speed...
						go func(buff []byte, off int) {
							dfi := rand.Intn(len(devfiles))
							//							newbuff := make([]byte, read_size)
							n, err := devfiles[dfi].ReadAt(buff, int64(off))
							if n != len(buff) || err != nil {
								fmt.Printf("Error reading file %d %v\n", n, err)
							}

							wg.Done()
							buffers <- buff
						}(<-buffers, offsets[i])
					}

					b.SetBytes(readSize)

					wg.Wait()
				})
			}
		}
	}
}

func BenchmarkDevWriteNL(b *testing.B) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	diskSize := 1024 * 1024 * 1024 * 4

	// Setup...
	// Lets simulate a little latency here
	store := sources.NewMemoryStorage(diskSize)
	//	store_latency := modules.NewArtificialLatency(store, 100*time.Millisecond, 0, 100*time.Millisecond, 0)
	driver := modules.NewMetrics(store)

	n := NewExposedStorageNBDNL(driver, 1, 0, driver.Size(), 4096, true)

	err = n.Init()
	if err != nil {
		panic(err)
	}

	/**
	* Cleanup everything
	*
	 */
	b.Cleanup(func() {
		err := n.Shutdown()
		if err != nil {
			fmt.Printf("Error cleaning up %v\n", err)
		}
	})

	driver.ResetMetrics() // Only start counting from now...

	// Here's the actual benchmark...

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", n.deviceIndex), os.O_RDWR, 0666)
	if err != nil {
		panic("Error opening dev file\n")
	}

	var wg sync.WaitGroup
	concurrent := make(chan bool, 100) // Do max 100 writes concurrently

	// Now do some timing...
	b.ResetTimer()
	ctime := time.Now()

	offset := int64(0)
	totalData := int64(0)
	writeSize := int64(4096)

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		concurrent <- true
		length := writeSize
		offset += writeSize
		if offset+length >= int64(diskSize) {
			offset = 0
		}
		totalData += length

		// Test write speed...
		go func(fOffset int64, fLength int64) {
			buffer := make([]byte, fLength)
			_, err := devfile.WriteAt(buffer, fOffset)
			if err != nil {
				fmt.Printf("Error writing file %v\n", err)
			}

			wg.Done()
			<-concurrent
		}(offset, length)
	}

	b.SetBytes(writeSize)

	wg.Wait()

	// Flush things...
	err = devfile.Sync()
	if err != nil {
		fmt.Printf("Error performing sync %v\n", err)
	}

	err = devfile.Close()
	if err != nil {
		fmt.Printf("Error closing dev %v\n", err)
	}

	fmt.Printf("Total Data %d from %d ops in %dms\n", totalData, b.N, time.Since(ctime).Milliseconds())
	driver.ShowStats("stats")

}
