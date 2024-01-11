package expose

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

const NBDdevice = "nbd2"

/**
 * Setup a disk with some files created.
 *
 */
func setup(dispatch NBDDispatcher, prov storage.StorageProvider, fileMin uint64, fileMax uint64, maxSize uint64) (storage.ExposedStorage, []string, error) {
	defer func() {
		//		fmt.Printf("umount\n")
		cmd := exec.Command("umount", fmt.Sprintf("/dev/%s", NBDdevice))
		err := cmd.Run()
		if err != nil {
			fmt.Printf("Error cleaning up unount %v\n", err)
		}

		err = os.Remove(fmt.Sprintf("/mnt/setup%s", NBDdevice))
		if err != nil {
			fmt.Printf("Error cleaning up remove %v\n", err)
		}
	}()

	p, err := NewNBD(dispatch, fmt.Sprintf("/dev/%s", NBDdevice))
	if err != nil {
		return nil, nil, err
	}

	go func() {
		err := p.Handle(prov)
		if err != nil {
			fmt.Printf("p.Handle returned %v\n", err)
		}
	}()

	p.WaitReady()

	err = os.Mkdir(fmt.Sprintf("/mnt/setup%s", NBDdevice), 0600)
	if err != nil {
		return nil, nil, err
	}

	//	fmt.Printf("mkfs.ext4\n")
	cmd := exec.Command("mkfs.ext4", fmt.Sprintf("/dev/%s", NBDdevice))
	err = cmd.Run()
	if err != nil {
		return nil, nil, err
	}

	//	fmt.Printf("mount\n")
	cmd = exec.Command("mount", fmt.Sprintf("/dev/%s", NBDdevice), fmt.Sprintf("/mnt/setup%s", NBDdevice))
	err = cmd.Run()
	if err != nil {
		return nil, nil, err
	}

	totalData := 0
	files := make([]string, 0)
	fileno := 0
	// Create some files fill up the disk...
	for {
		length := int(fileMin)
		if fileMax > fileMin {
			length += rand.Intn(int(fileMax - fileMin))
		}

		if totalData > int(maxSize) {
			// We have enough data...
			break
		}

		randomData := make([]byte, length)
		crand.Read(randomData)
		name := fmt.Sprintf("/mnt/setup%s/file_%d", NBDdevice, fileno)
		err := os.WriteFile(name, randomData, 0600)
		if err != nil {
			// Remove the file if it was created...
			os.Remove(name)

			break // Out of disk space maybe. FIXME
		}

		totalData += length

		files = append(files, fmt.Sprintf("/mnt/bench%s/file_%d", NBDdevice, fileno))
		fileno++
	}

	return p, files, nil
}

func shutdown(p storage.ExposedStorage) error {
	//	fmt.Printf("umount\n")
	cmd := exec.Command("umount", fmt.Sprintf("/dev/%s", NBDdevice))
	err := cmd.Run()
	if err != nil {
		return err
	}
	err = os.Remove(fmt.Sprintf("/mnt/bench%s", NBDdevice))
	if err != nil {
		return err
	}

	err = p.Shutdown()
	if err != nil {
		return err
	}
	return nil
}

type TestConfig struct {
	Name    string
	Size    uint64
	FileMin uint64
	FileMax uint64
}

func BenchmarkRead(mb *testing.B) {

	tests := []TestConfig{
		{"BigFiles", 100 * 1024 * 1024, 20 * 1024 * 1024, 20 * 1024 * 1024},
		{"MediumFiles", 100 * 1024 * 1024, 1024 * 1024, 1024 * 1024},
		{"SmallFiles", 100 * 1024 * 1024, 8 * 1024, 8 * 1024},
	}

	dispatchers := []string{
		"simple", "dispatch",
	}

	for _, dt := range dispatchers {
		for _, c := range tests {

			mb.Run(fmt.Sprintf("%s_%s", dt, c.Name), func(b *testing.B) {

				// Setup...
				driver := modules.NewMetrics(sources.NewMemoryStorage(int(c.Size)))

				var d NBDDispatcher
				if dt == "simple" {
					d = NewDispatchSimple()
				} else if dt == "dispatch" {
					d = NewDispatch()
				}
				p, files, err := setup(d, driver, c.FileMin, c.FileMax, uint64(float64(c.Size)*0.8))
				if err != nil {
					fmt.Printf("Error setup %v\n", err)
					return
				}

				// Now mount the disk...
				err = os.Mkdir(fmt.Sprintf("/mnt/bench%s", NBDdevice), 0600)
				if err != nil {
					panic(err)
				}

				//				fmt.Printf("mount\n")
				cmd := exec.Command("mount", fmt.Sprintf("/dev/%s", NBDdevice), fmt.Sprintf("/mnt/bench%s", NBDdevice))
				err = cmd.Run()
				if err != nil {
					panic(err)
				}

				/**
				 * Cleanup everything
				 *
				 */
				b.Cleanup(func() {
					shutdown(p)
					driver.ShowStats("stats")
				})

				driver.ResetMetrics() // Only start counting from now...

				// Here's the actual benchmark...

				var wg sync.WaitGroup
				concurrent := make(chan bool, 32)

				// Now do some timing...
				b.ResetTimer()

				totalData := int64(0)

				for i := 0; i < b.N; i++ {
					fileno := rand.Intn(len(files))
					name := files[fileno]

					fi, err := os.Stat(name)
					if err != nil {
						fmt.Printf("Error statting file %v\n", err)
					} else {

						wg.Add(1)
						concurrent <- true
						totalData += fi.Size()

						// Test read speed...
						go func(filename string) {

							data, err := os.ReadFile(filename)
							if err != nil {
								fmt.Printf("Error reading file %v\n", err)
							}

							if len(data) == 0 {
								fmt.Printf("Warning: The file %s was not there\n", filename)
							}

							wg.Done()
							<-concurrent
						}(name)
					}
				}

				b.SetBytes(totalData)

				wg.Wait()

				fmt.Printf("Total Data %d\n", totalData)

			})
		}
	}
}

func BenchmarkWrite(mb *testing.B) {
	tests := []TestConfig{
		{"BigFiles", 100 * 1024 * 1024, 20 * 1024 * 1024, 20 * 1024 * 1024},
		{"MediumFiles", 100 * 1024 * 1024, 1024 * 1024, 1024 * 1024},
		{"SmallFiles", 100 * 1024 * 1024, 8 * 1024, 8 * 1024},
	}

	dispatchers := []string{
		"simple", "dispatch",
	}

	for _, dt := range dispatchers {
		for _, c := range tests {

			mb.Run(fmt.Sprintf("%s_%s", dt, c.Name), func(b *testing.B) {

				// Setup...
				driver := modules.NewMetrics(sources.NewMemoryStorage(int(c.Size)))

				var d NBDDispatcher
				if dt == "simple" {
					d = NewDispatchSimple()
				} else if dt == "dispatch" {
					d = NewDispatch()
				}
				p, files, err := setup(d, driver, c.FileMin, c.FileMax, uint64(float64(c.Size)*0.8))

				if err != nil {
					fmt.Printf("Error setup %v\n", err)
					return
				}

				// Now mount the disk...
				err = os.Mkdir(fmt.Sprintf("/mnt/bench%s", NBDdevice), 0600)
				if err != nil {
					panic(err)
				}

				cmd := exec.Command("mount", fmt.Sprintf("/dev/%s", NBDdevice), fmt.Sprintf("/mnt/bench%s", NBDdevice))
				err = cmd.Run()
				if err != nil {
					panic(err)
				}

				/**
				 * Cleanup everything
				 *
				 */
				b.Cleanup(func() {
					shutdown(p)
					driver.ShowStats("stats")
				})

				driver.ResetMetrics()

				// Here's the actual benchmark...

				var wg sync.WaitGroup
				concurrent := make(chan bool, 32)

				// Now do some timing...
				b.ResetTimer()

				totalData := int64(0)

				for i := 0; i < b.N; i++ {
					fileno := rand.Intn(len(files))
					name := files[fileno]

					fi, err := os.Stat(name)
					if err != nil {
						fmt.Printf("Error statting file %v\n", err)
					} else {

						totalData += fi.Size()

						newData := make([]byte, fi.Size())
						crand.Read(newData)

						wg.Add(1)
						concurrent <- true

						// Test write speed... FIXME: Concurrent access to same file
						go func(filename string, data []byte) {

							af, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
							if err != nil {
								fmt.Printf("Error opening file %v\n", err)
							} else {
								_, err = af.Write(data)
								if err != nil {
									fmt.Printf("Error writing file %v\n", err)
									time.Sleep(1 * time.Minute)
								}
								err = af.Close()
								if err != nil {
									fmt.Printf("Error closing file %v\n", err)
								}
							}

							//err := os.WriteFile(filename, data, 0600)
							if err != nil {
								fmt.Printf("Error writing file %v\n", err)
							}

							wg.Done()
							<-concurrent
						}(name, newData)
					}
				}

				b.SetBytes(totalData)

				wg.Wait()

				fmt.Printf("Total Data %d\n", totalData)
			})
		}
	}
}
