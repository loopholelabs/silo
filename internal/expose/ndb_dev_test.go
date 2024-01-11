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
	NBDdevice := "nbd1"
	diskSize := 1024 * 1024 * 1024 * 4

	dispatchers := []string{
		"simple", "dispatch",
	}

	for _, dt := range dispatchers {

		mb.Run(fmt.Sprintf("%s_devRead", dt), func(b *testing.B) {

			// Setup...
			// Lets simulate a little latency
			store := sources.NewMemoryStorage(int(diskSize))
			store_latency := modules.NewArtificialLatency(store, 100*time.Millisecond, 100*time.Millisecond)
			driver := modules.NewMetrics(store_latency)

			var d NBDDispatcher
			if dt == "simple" {
				d = NewDispatchSimple()
			} else if dt == "dispatch" {
				d = NewDispatch()
			}

			p, err := NewNBD(d, fmt.Sprintf("/dev/%s", NBDdevice))
			if err != nil {
				panic(err)
			}

			go func() {
				err := p.Handle(driver)
				if err != nil {
					fmt.Printf("p.Handle returned %v\n", err)
				}
			}()

			p.WaitReady()

			/**
			* Cleanup everything
			*
			 */
			b.Cleanup(func() {
				err = p.Shutdown()
				if err != nil {
					fmt.Printf("Error cleaning up %v\n", err)
				}
				driver.ShowStats("stats")
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

			offset := int64(0)
			totalData := int64(0)

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				concurrent <- true
				length := int64(4096)
				offset += 4096
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

			b.SetBytes(totalData)

			wg.Wait()

			fmt.Printf("Total Data %d\n", totalData)
		})
	}
}

func BenchmarkDevWrite(mb *testing.B) {
	NBDdevice := "nbd2"
	diskSize := 1024 * 1024 * 1024 * 4

	dispatchers := []string{
		"simple", "dispatch",
	}

	for _, dt := range dispatchers {

		mb.Run(fmt.Sprintf("%s_devWrite", dt), func(b *testing.B) {

			// Setup...
			// Lets simulate a little latency
			store := sources.NewMemoryStorage(int(diskSize))
			store_latency := modules.NewArtificialLatency(store, 100*time.Millisecond, 100*time.Millisecond)
			driver := modules.NewMetrics(store_latency)

			var d NBDDispatcher
			if dt == "simple" {
				d = NewDispatchSimple()
			} else if dt == "dispatch" {
				d = NewDispatch()
			}

			p, err := NewNBD(d, fmt.Sprintf("/dev/%s", NBDdevice))
			if err != nil {
				panic(err)
			}

			go func() {
				err := p.Handle(driver)
				if err != nil {
					fmt.Printf("p.Handle returned %v\n", err)
				}
			}()

			p.WaitReady()

			/**
			* Cleanup everything
			*
			 */
			b.Cleanup(func() {
				err = p.Shutdown()
				if err != nil {
					fmt.Printf("Error cleaning up %v\n", err)
				}
				driver.ShowStats("stats")
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

			offset := int64(0)
			totalData := int64(0)

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				concurrent <- true
				length := int64(4096)
				offset += 4096
				if offset+length >= int64(diskSize) {
					offset = 0
				}
				totalData += length

				// Test read speed...
				go func(f_offset int64, f_length int64) {
					buffer := make([]byte, f_length)
					_, err := devfile.WriteAt(buffer, f_offset)
					if err != nil {
						fmt.Printf("Error writing file %d %d | %v\n", f_offset, f_length, err)
					}

					wg.Done()
					<-concurrent
				}(offset, length)
			}

			err = devfile.Sync()
			if err != nil {
				fmt.Printf("Error syncing %v\n", err)
			}

			b.SetBytes(totalData)

			wg.Wait()

			fmt.Printf("Total Data %d\n", totalData)
		})
	}
}
