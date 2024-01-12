package sources

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
)

type sourceInfo struct {
	Name   string
	Source storage.StorageProvider
}

func BenchmarkSourcesRead(mb *testing.B) {
	sources := make([]sourceInfo, 0)

	// Create some sources to test...
	sources = append(sources, sourceInfo{"MemoryStorage", NewMemoryStorage(1024 * 1024 * 4)})
	cr := func(s int) storage.StorageProvider {
		return NewMemoryStorage(s)
	}
	sources = append(sources, sourceInfo{"ShardedMemoryStorage", modules.NewShardedStorage(1024*1024*4, 1024*4, cr)})

	fileStorage, err := NewFileStorage("test_data", 1024*1024*4)
	if err != nil {
		panic(err)
	}
	defer func() {
		fileStorage.Close()
		os.Remove("test_data")
	}()
	sources = append(sources, sourceInfo{"FileStorage", fileStorage})

	// Test some different sources read speed...
	for _, s := range sources {

		mb.Run(s.Name, func(b *testing.B) {

			// Do some concurrent reads...
			var wg sync.WaitGroup
			concurrency := make(chan bool, 1024)
			var totalData int64 = 0
			for i := 0; i < b.N; i++ {
				concurrency <- true
				wg.Add(1)
				go func() {
					buffer := make([]byte, 4096)
					offset := rand.Intn(int(s.Source.Size()) - len(buffer))
					n, err := s.Source.ReadAt(buffer, int64(offset))
					if n != len(buffer) || err != nil {
						panic(err)
					}
					atomic.AddInt64(&totalData, int64(n))
					wg.Done()
					<-concurrency
				}()
			}

			wg.Wait()

			b.SetBytes(int64(totalData))
		})
	}
}

func BenchmarkSourcesWrite(mb *testing.B) {
	sources := make([]sourceInfo, 0)

	// Create some sources to test...
	sources = append(sources, sourceInfo{"MemoryStorage", NewMemoryStorage(1024 * 1024 * 4)})
	cr := func(s int) storage.StorageProvider {
		return NewMemoryStorage(s)
	}
	sources = append(sources, sourceInfo{"ShardedMemoryStorage", modules.NewShardedStorage(1024*1024*4, 1024*4, cr)})

	fileStorage, err := NewFileStorage("test_data", 1024*1024*4)
	if err != nil {
		panic(err)
	}
	defer func() {
		fileStorage.Close()
		os.Remove("test_data")
	}()
	sources = append(sources, sourceInfo{"FileStorage", fileStorage})

	// Do sharded files...
	sharded_files := make(map[string]*FileStorage)

	crf := func(s int) storage.StorageProvider {
		name := fmt.Sprintf("test_data_shard_%d", len(sharded_files))
		fs, err := NewFileStorage(name, int64(s))
		if err != nil {
			panic(err)
		}
		sharded_files[name] = fs
		return NewMemoryStorage(s)
	}
	sources = append(sources, sourceInfo{"ShardedFileStorage", modules.NewShardedStorage(1024*1024*4, 1024*4, crf)})
	defer func() {
		for f, ms := range sharded_files {
			ms.Close()
			os.Remove(f)
		}
	}()

	// Test some different sources read speed...
	for _, s := range sources {

		mb.Run(s.Name, func(b *testing.B) {

			// Do some concurrent reads...
			var wg sync.WaitGroup
			concurrency := make(chan bool, 1024)
			var totalData int64 = 0
			for i := 0; i < b.N; i++ {
				concurrency <- true
				wg.Add(1)
				go func() {
					buffer := make([]byte, 4096)
					offset := rand.Intn(int(s.Source.Size()) - len(buffer))
					n, err := s.Source.WriteAt(buffer, int64(offset))
					if n != len(buffer) || err != nil {
						panic(err)
					}
					atomic.AddInt64(&totalData, int64(n))
					wg.Done()
					<-concurrency
				}()
			}

			wg.Wait()

			b.SetBytes(int64(totalData))
		})
	}
}
