package expose

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

func BenchmarkDevRead(mb *testing.B) {

	sizes := []int64{4096, 65536, 1024 * 1024}

	for _, v := range sizes {
		name := fmt.Sprintf("blocksize_%d", v)
		mb.Run(name, func(b *testing.B) {
			NBDdevice := "nbd1"
			diskSize := 1024 * 1024 * 1024 * 4

			// Setup...
			// Lets simulate a little latency here
			store := sources.NewMemoryStorage(int(diskSize))
			//store_latency := modules.NewArtificialLatency(store, 100*time.Millisecond, 0, 100*time.Millisecond, 0)
			driver := modules.NewMetrics(store)

			n := NewExposedStorageNBD(driver, NBDdevice, 1, 0, uint64(driver.Size()), 4096, 0)

			go func() {
				err := n.Handle()
				if err != nil {
					panic(err)
				}
			}()

			n.WaitReady()

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

			devfile, err := os.OpenFile(fmt.Sprintf("/dev/%s", NBDdevice), os.O_RDWR, 0666)
			if err != nil {
				panic("Error opening dev file\n")
			}

			b.Cleanup(func() {
				devfile.Close()
			})

			var wg sync.WaitGroup
			concurrent := make(chan bool, 100) // Do max 100 reads concurrently

			// Now do some timing...
			b.ResetTimer()
			ctime := time.Now()

			read_size := int64(v)
			offset := int64(0)
			totalData := int64(0)

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				concurrent <- true
				length := read_size
				offset += read_size
				if offset+length >= int64(diskSize) {
					offset = 0
				}
				totalData += length

				// Test read speed...
				go func(f_offset int64, f_length int64) {
					buffer := make([]byte, f_length)
					_, err := devfile.ReadAt(buffer, f_offset)
					if err != nil {
						fmt.Printf("Error reading file %v\n", err)
					}

					wg.Done()
					<-concurrent
				}(offset, length)
			}

			b.SetBytes(int64(read_size))

			wg.Wait()

			duration := time.Since(ctime).Seconds()
			mb_per_sec := float64(totalData) / (duration * 1024 * 1024)

			fmt.Printf("Total Data %d from %d ops in %dms RATE %.2fMB/s\n", totalData, b.N, time.Since(ctime).Milliseconds(), mb_per_sec)
			driver.ShowStats("stats")
		})
	}
}

func BenchmarkDevWrite(b *testing.B) {
	NBDdevice := "nbd1"
	diskSize := 1024 * 1024 * 1024 * 4

	// Setup...
	// Lets simulate a little latency here
	store := sources.NewMemoryStorage(int(diskSize))
	//	store_latency := modules.NewArtificialLatency(store, 100*time.Millisecond, 0, 100*time.Millisecond, 0)
	driver := modules.NewMetrics(store)

	n := NewExposedStorageNBD(driver, NBDdevice, 1, 0, uint64(driver.Size()), 4096, 0)

	go func() {
		err := n.Handle()
		if err != nil {
			panic(err)
		}
	}()

	n.WaitReady()

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

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/%s", NBDdevice), os.O_RDWR, 0666)
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
	write_size := int64(4096)

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		concurrent <- true
		length := write_size
		offset += write_size
		if offset+length >= int64(diskSize) {
			offset = 0
		}
		totalData += length

		// Test write speed...
		go func(f_offset int64, f_length int64) {
			buffer := make([]byte, f_length)
			_, err := devfile.WriteAt(buffer, f_offset)
			if err != nil {
				fmt.Printf("Error writing file %v\n", err)
			}

			wg.Done()
			<-concurrent
		}(offset, length)
	}

	b.SetBytes(int64(write_size))

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
