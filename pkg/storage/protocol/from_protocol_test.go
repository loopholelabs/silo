package protocol

import (
	"context"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/stretchr/testify/assert"
)

func TestFromProtocol(t *testing.T) {

	ctx, cancelfn := context.WithCancel(context.TODO())

	proto := NewMockProtocol(ctx)

	factory := func(_ *packets.DevInfo) storage.Provider {
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

	// Should get here ok
	assert.True(t, true)
}
