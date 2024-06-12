package protocol

import (
	"context"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

func TestFromProtocol(t *testing.T) {

	ctx, cancelfn := context.WithCancel(context.TODO())

	proto := NewMockProtocol(ctx)

	factory := func(di *packets.DevInfo) storage.StorageProvider {
		return nil
	}

	from := NewFromProtocol(ctx, 1, factory, proto)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		_ = from.HandleReadAt()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		_ = from.HandleWriteAt()
		wg.Done()
	}()

	cancelfn()

	// Both should quit now...
	wg.Wait()
}
