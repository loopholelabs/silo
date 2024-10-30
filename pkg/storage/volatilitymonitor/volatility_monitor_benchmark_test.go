package volatilitymonitor

import (
	"sync"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

const benchSize = 4 * 1024 * 1024
const benchBlockSize = 4096
const benchNumBlocks = benchSize / benchBlockSize

func benchOrder(mb *testing.B, ord storage.BlockOrder) {
	mb.ResetTimer()

	for i := 0; i < mb.N; i++ {
		ord.AddAll()

		var wg sync.WaitGroup
		for j := 0; j < benchNumBlocks; j++ {
			wg.Add(1)
			go func() {
				ord.GetNext()
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkBlockOrderVol(mb *testing.B) {
	// Create a new block storage, backed by memory storage
	mem := sources.NewMemoryStorage(benchSize)
	vol := NewVolatilityMonitor(mem, benchBlockSize, 50*time.Millisecond)

	benchOrder(mb, vol)
}

func BenchmarkBlockOrderAny(mb *testing.B) {
	ord := blocks.NewAnyBlockOrder(benchNumBlocks, nil)
	benchOrder(mb, ord)
}

func BenchmarkBlockOrderAnyPriority(mb *testing.B) {
	ord1 := blocks.NewAnyBlockOrder(mb.N, nil)
	ord := blocks.NewPriorityBlockOrder(mb.N, ord1)
	benchOrder(mb, ord)
}
