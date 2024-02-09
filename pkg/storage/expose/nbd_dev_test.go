package expose

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

func BenchmarkDevRead(b *testing.B) {
	NBDdevice := "nbd1"
	diskSize := 1024 * 1024 * 1024 * 4

	// Setup...
	// Lets simulate a little latency here
	store := sources.NewMemoryStorage(int(diskSize))
	//store_latency := modules.NewArtificialLatency(store, 100*time.Millisecond, 0, 100*time.Millisecond, 0)
	driver := modules.NewMetrics(store)

	n := NewExposedStorageNBD(driver, NBDdevice, 1, 0, uint64(driver.Size()), 4096, 0)

	go func() {
		err := n.Start()
		if err != nil {
			panic(err)
		}
	}()

	n.Ready()

	/**
	* Cleanup everything
	*
	 */
	b.Cleanup(func() {
		err := n.Disconnect()
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

	read_size := 4096
	offset := int64(0)
	totalData := int64(0)

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		concurrent <- true
		length := int64(read_size)
		offset += int64(read_size)
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

	fmt.Printf("Total Data %d from %d ops\n", totalData, b.N)
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
		err := n.Start()
		if err != nil {
			panic(err)
		}
	}()

	n.Ready()

	/**
	* Cleanup everything
	*
	 */
	b.Cleanup(func() {
		err := n.Disconnect()
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

	/*
		b.Cleanup(func() {
			devfile.Close()
		})
	*/

	var wg sync.WaitGroup
	concurrent := make(chan bool, 100) // Do max 100 writes concurrently

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

	b.SetBytes(totalData)

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

	fmt.Printf("Total Data %d from %d ops\n", totalData, b.N)
}
