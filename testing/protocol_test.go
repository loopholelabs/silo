package storage

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestProtocolWriteAt(t *testing.T) {
	size := 1024 * 1024
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	pr := protocol.NewMockProtocol()

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *protocol.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := modules.NewFromProtocol(1, storeFactory, pr)

	// Now do some things and make sure they happen...

	// TODO: Shutdown...
	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	// Send devInfo
	sourceToProtocol.SendDevInfo("test")

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
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	buff := make([]byte, 4096)
	rand.Read(buff)

	pr := protocol.NewMockProtocol()

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *protocol.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))

		n, err := store.WriteAt(buff, 12)

		assert.NoError(t, err)
		assert.Equal(t, len(buff), n)

		return store
	}

	destFromProtocol := modules.NewFromProtocol(1, storeFactory, pr)

	// Now do some things and make sure they happen...

	// TODO: Shutdown...
	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	sourceToProtocol.SendDevInfo("test")

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err := sourceToProtocol.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}

func TestProtocolRWWriteAt(t *testing.T) {
	fmt.Printf("TestProtocolRWWriteAt\n")
	size := 1024 * 1024
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	destDev := make(chan uint32, 8)

	prSource := protocol.NewProtocolRW(context.TODO(), r1, w2, nil)
	prDest := protocol.NewProtocolRW(context.TODO(), r2, w1, func(p protocol.Protocol, dev uint32) {
		destDev <- dev
	})

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, prSource)

	storeFactory := func(di *protocol.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := modules.NewFromProtocol(1, storeFactory, prDest)

	// TODO: Cleanup
	go prSource.Handle()
	go prDest.Handle()

	// Now do some things and make sure they happen...

	// TODO: Shutdown...
	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	sourceToProtocol.SendDevInfo("test")

	// Should know the dev now...
	assert.Equal(t, uint32(1), <-destDev)
	assert.Equal(t, 0, len(destDev))

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
	var store storage.StorageProvider

	buff := make([]byte, 4096)
	rand.Read(buff)

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	prSource := protocol.NewProtocolRW(context.TODO(), r1, w2, nil)
	prDest := protocol.NewProtocolRW(context.TODO(), r2, w1, nil)

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, prSource)

	storeFactory := func(di *protocol.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		n, err := store.WriteAt(buff, 12)

		assert.NoError(t, err)
		assert.Equal(t, len(buff), n)

		return store
	}

	destFromProtocol := modules.NewFromProtocol(1, storeFactory, prDest)

	// TODO: Cleanup
	go prSource.Handle()
	go prDest.Handle()

	// Now do some things and make sure they happen...

	// TODO: Shutdown...
	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	sourceToProtocol.SendDevInfo("test")

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err := sourceToProtocol.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}

func TestProtocolEvents(t *testing.T) {
	size := 1024 * 1024
	var store storage.StorageProvider

	pr := protocol.NewMockProtocol()

	sourceToProtocol := modules.NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *protocol.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := modules.NewFromProtocol(1, storeFactory, pr)

	events := make(chan protocol.EventType, 10)

	// Now do some things and make sure they happen...
	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleEvent(func(e protocol.EventType) {
		events <- e
	})
	go destFromProtocol.HandleSend(context.TODO())

	// Send devInfo
	sourceToProtocol.SendDevInfo("test")

	// Send some events and make sure they happen at the other end...

	sourceToProtocol.SendEvent(protocol.EventPreLock)
	// There should be the event waiting for us already.
	assert.Equal(t, 1, len(events))
	e := <-events
	assert.Equal(t, protocol.EventPreLock, e)
	sourceToProtocol.SendEvent(protocol.EventPostLock)
	sourceToProtocol.SendEvent(protocol.EventPreUnlock)
	sourceToProtocol.SendEvent(protocol.EventPostUnlock)
	sourceToProtocol.SendEvent(protocol.EventCompleted)
	e = <-events
	assert.Equal(t, protocol.EventPostLock, e)
	e = <-events
	assert.Equal(t, protocol.EventPreUnlock, e)
	e = <-events
	assert.Equal(t, protocol.EventPostUnlock, e)
	e = <-events
	assert.Equal(t, protocol.EventCompleted, e)

}
