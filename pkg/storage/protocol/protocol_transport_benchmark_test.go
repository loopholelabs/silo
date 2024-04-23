package protocol

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

const size = 1024 * 1024

func setup(num int) *ToProtocol {
	var store storage.StorageProvider

	readers1 := make([]io.Reader, 0)
	readers2 := make([]io.Reader, 0)
	writers1 := make([]io.Writer, 0)
	writers2 := make([]io.Writer, 0)

	for i := 0; i < num; i++ {
		r1, w1 := io.Pipe()
		r2, w2 := io.Pipe()
		readers1 = append(readers1, r1)
		writers1 = append(writers1, w1)

		readers2 = append(readers2, r2)
		writers2 = append(writers2, w2)
	}

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		cr := func(i int, size int) (storage.StorageProvider, error) {
			return sources.NewMemoryStorage(int(di.Size)), nil
		}
		var err error
		store, err = modules.NewShardedStorage(int(di.Size), 1024, cr)
		if err != nil {
			panic(err)
		}
		return store
	}

	prSource := NewProtocolRW(context.TODO(), readers1, writers2, nil)
	prDest := NewProtocolRW(context.TODO(), readers2, writers1, func(p Protocol, dev uint32) {
		destFromProtocol := NewFromProtocol(dev, storeFactory, p)
		go destFromProtocol.HandleDevInfo()
		go destFromProtocol.HandleReadAt()
		go destFromProtocol.HandleWriteAt()
		go destFromProtocol.HandleWriteAtComp()

	})

	sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

	go prSource.Handle()
	go prDest.Handle()

	sourceToProtocol.SendDevInfo("test", 1024*1024)
	return sourceToProtocol
}

func BenchmarkWriteAt(mb *testing.B) {
	sourceToProtocol := setup(1)

	// Do some writes
	buff := make([]byte, 256*1024)
	rand.Read(buff)

	mb.ReportAllocs()
	mb.SetBytes(int64(len(buff)))
	mb.ResetTimer()

	p := 0

	for i := 0; i < mb.N; i++ {
		n, err := sourceToProtocol.WriteAt(buff, int64(p))
		if err != nil || n != len(buff) {
			panic(err)
		}
		p += 1024
		if p+len(buff) > size {
			p = 0
		}
	}
}

func BenchmarkWriteAtComp(mb *testing.B) {
	sourceToProtocol := setup(1)

	sourceToProtocol.CompressedWrites = true

	// Do some writes
	buff := make([]byte, 256*1024)
	// NB: Make them just zeros so they compress v well...
	//	rand.Read(buff)

	mb.ReportAllocs()
	mb.SetBytes(int64(len(buff)))
	mb.ResetTimer()

	p := 0

	for i := 0; i < mb.N; i++ {
		n, err := sourceToProtocol.WriteAt(buff, int64(p))
		if err != nil || n != len(buff) {
			panic(err)
		}
		p += 1024
		if p+len(buff) > size {
			p = 0
		}
	}
}

func BenchmarkWriteAtConcurrent(mb *testing.B) {
	bufferSizes := []int{4 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024}

	maxConcurrent := 16

	sourceToProtocol := setup(maxConcurrent)

	for _, bSize := range bufferSizes {

		mb.Run(fmt.Sprintf("buffer_%d", bSize), func(b *testing.B) {
			// Do some writes concurrently
			buff := make([]byte, bSize)
			rand.Read(buff)

			b.ReportAllocs()
			b.SetBytes(int64(len(buff)))
			b.ResetTimer()

			var wg sync.WaitGroup
			concurrency := make(chan bool, maxConcurrent)

			p := 0

			for i := 0; i < b.N; i++ {
				concurrency <- true
				wg.Add(1)
				go func(ptr int64) {
					n, err := sourceToProtocol.WriteAt(buff, ptr)
					if err != nil || n != len(buff) {
						panic(err)
					}
					wg.Done()
					<-concurrency
				}(int64(p))
				p += len(buff) // Move forward...
				if p+len(buff) > size {
					p = 0
				}
			}

			wg.Wait()
		})
	}
}
