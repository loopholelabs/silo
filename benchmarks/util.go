package benchmarks

import (
	crand "crypto/rand"
	"math/rand"
	"sync"
)

/**
 * Perform some random ops
 *
 */
func PerformRandomOp(size uint64, concurrency int, blockSize int, num int, op func([]byte, int64) error) error {
	con := make(chan bool, concurrency)
	var wg sync.WaitGroup
	errs := make(chan error, 1)

	for i := 0; i < num; i++ {
		con <- true
		wg.Add(1)
		go func() {
			buffer := make([]byte, blockSize)
			crand.Read(buffer)
			offset := rand.Intn(int(size) - blockSize)

			err := op(buffer, int64(offset))

			if err != nil {
				// Save the first error
				select {
				case errs <- err:
				default:
				}
			}
			<-con
			wg.Done()
		}()
	}

	// Wait for any pending ones...
	wg.Wait()

	// Check if there was an error
	select {
	case err := <-errs:
		return err
	default:
	}

	return nil
}
