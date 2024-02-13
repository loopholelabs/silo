package storage

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestProtocolWriteAt(t *testing.T) {
	size := 1024 * 1024
	store := sources.NewMemoryStorage(size)

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	pr := protocol.NewMockProtocol()

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, pr)

	destFromProtocol := modules.NewFromProtocol(1, store, pr)

	// Now do some things and make sure they happen...

	// TODO: Shutdown...
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	buff := make([]byte, 4096)
	rand.Read(buff)
	n, err := sourceToProtocol.WriteAt(buff, 12)

	assert.NoError(t, err)
	assert.Equal(t, len(buff), n)

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err = store.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}

func TestProtocolReadAt(t *testing.T) {
	size := 1024 * 1024
	store := sources.NewMemoryStorage(size)

	buff := make([]byte, 4096)
	rand.Read(buff)
	n, err := store.WriteAt(buff, 12)

	assert.NoError(t, err)
	assert.Equal(t, len(buff), n)

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	pr := protocol.NewMockProtocol()

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, pr)

	destFromProtocol := modules.NewFromProtocol(1, store, pr)

	// Now do some things and make sure they happen...

	// TODO: Shutdown...
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err = sourceToProtocol.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}

func TestProtocolRWWriteAt(t *testing.T) {
	size := 1024 * 1024
	store := sources.NewMemoryStorage(size)

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	prSource := protocol.NewProtocolRW(context.TODO(), r1, w2)
	prDest := protocol.NewProtocolRW(context.TODO(), r2, w1)

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, prSource)

	destFromProtocol := modules.NewFromProtocol(1, store, prDest)

	// TODO: Cleanup
	go prSource.Handle()
	go prDest.Handle()

	// Now do some things and make sure they happen...

	// TODO: Shutdown...
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	buff := make([]byte, 4096)
	rand.Read(buff)
	n, err := sourceToProtocol.WriteAt(buff, 12)

	assert.NoError(t, err)
	assert.Equal(t, len(buff), n)

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err = store.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}

func TestProtocolRWReadAt(t *testing.T) {
	size := 1024 * 1024
	store := sources.NewMemoryStorage(size)

	buff := make([]byte, 4096)
	rand.Read(buff)
	n, err := store.WriteAt(buff, 12)

	assert.NoError(t, err)
	assert.Equal(t, len(buff), n)

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	prSource := protocol.NewProtocolRW(context.TODO(), r1, w2)
	prDest := protocol.NewProtocolRW(context.TODO(), r2, w1)

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, prSource)

	destFromProtocol := modules.NewFromProtocol(1, store, prDest)

	// TODO: Cleanup
	go prSource.Handle()
	go prDest.Handle()

	// Now do some things and make sure they happen...

	// TODO: Shutdown...
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err = sourceToProtocol.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}
