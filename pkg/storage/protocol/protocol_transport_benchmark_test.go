package protocol

import (
	"context"
	"crypto/rand"
	"io"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
)

const size = 1024 * 1024

func setup() *ToProtocol {
	var store storage.StorageProvider

	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	prSource := NewProtocolRW(context.TODO(), r1, w2, nil)
	prDest := NewProtocolRW(context.TODO(), r2, w1, func(p Protocol, dev uint32) {})

	sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

	storeFactory := func(di *DevInfo) storage.StorageProvider {
		cr := func(size int) storage.StorageProvider {
			return sources.NewMemoryStorage(int(di.Size))
		}
		store = modules.NewShardedStorage(int(di.Size), 1024, cr)
		return store
	}

	destFromProtocol := NewFromProtocol(1, storeFactory, prDest)

	go prSource.Handle()
	go prDest.Handle()

	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	sourceToProtocol.SendDevInfo("test", 1024*1024)
	return sourceToProtocol
}

func BenchmarkWriteAt(mb *testing.B) {
	sourceToProtocol := setup()

	// Do some writes
	buff := make([]byte, 4096)
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

func BenchmarkWriteAtConcurrent(mb *testing.B) {
	sourceToProtocol := setup()

	// Do some writes concurrently
	buff := make([]byte, 4096)
	rand.Read(buff)

	mb.ReportAllocs()
	mb.SetBytes(int64(len(buff)))
	mb.ResetTimer()

	var wg sync.WaitGroup
	concurrency := make(chan bool, 100)

	p := 0

	for i := 0; i < mb.N; i++ {
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
		p += 1024
		if p+len(buff) > size {
			p = 0
		}
	}

	wg.Wait()
}
