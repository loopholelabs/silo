package protocol

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestProtocolWriteAt(t *testing.T) {
	size := 1024 * 1024
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	pr := NewMockProtocol()

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(1, storeFactory, pr)

	// Now do some things and make sure they happen...

	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	// Send devInfo
	sourceToProtocol.SendDevInfo("test", 4096)

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

func TestProtocolWriteAtComp(t *testing.T) {
	size := 1024 * 1024
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	pr := NewMockProtocol()

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	sourceToProtocol.CompressedWrites = true

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(1, storeFactory, pr)

	// Now do some things and make sure they happen...

	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()
	go destFromProtocol.HandleWriteAtComp()

	// Send devInfo
	sourceToProtocol.SendDevInfo("test", 4096)

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

	pr := NewMockProtocol()

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))

		n, err := store.WriteAt(buff, 12)

		assert.NoError(t, err)
		assert.Equal(t, len(buff), n)

		return store
	}

	destFromProtocol := NewFromProtocol(1, storeFactory, pr)

	// Now do some things and make sure they happen...

	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	sourceToProtocol.SendDevInfo("test", 4096)

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err := sourceToProtocol.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}

func TestProtocolRWWriteAt(t *testing.T) {
	size := 1024 * 1024
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	destDev := make(chan uint32, 8)

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	prSource := NewProtocolRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := NewProtocolRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, func(p Protocol, dev uint32) {
		destDev <- dev
		destFromProtocol := NewFromProtocol(dev, storeFactory, p)

		go destFromProtocol.HandleDevInfo()
		go destFromProtocol.HandleReadAt()
		go destFromProtocol.HandleWriteAt()
	})

	sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

	// TODO: Cleanup
	go prSource.Handle()
	go prDest.Handle()

	// Now do some things and make sure they happen...

	sourceToProtocol.SendDevInfo("test", 4096)

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

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		n, err := store.WriteAt(buff, 12)

		assert.NoError(t, err)
		assert.Equal(t, len(buff), n)

		return store
	}

	initDev := func(p Protocol, dev uint32) {
		destFromProtocol := NewFromProtocol(dev, storeFactory, p)

		go destFromProtocol.HandleDevInfo()
		go destFromProtocol.HandleReadAt()
		go destFromProtocol.HandleWriteAt()
	}

	prSource := NewProtocolRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := NewProtocolRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, initDev)

	sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

	// TODO: Cleanup
	go prSource.Handle()
	go prDest.Handle()

	// Now do some things and make sure they happen...
	sourceToProtocol.SendDevInfo("test", 4096)

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

	pr := NewMockProtocol()

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(1, storeFactory, pr)

	events := make(chan packets.EventType, 10)

	// Now do some things and make sure they happen...
	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleEvent(func(e *packets.Event) {
		events <- e.Type
	})

	// Send devInfo
	sourceToProtocol.SendDevInfo("test", 4096)

	// Send some events and make sure they happen at the other end...

	sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventPreLock})
	// There should be the event waiting for us already.
	assert.Equal(t, 1, len(events))
	e := <-events
	assert.Equal(t, packets.EventPreLock, e)
	sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventPostLock})
	sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventPreUnlock})
	sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventPostUnlock})
	sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventCompleted})
	e = <-events
	assert.Equal(t, packets.EventPostLock, e)
	e = <-events
	assert.Equal(t, packets.EventPreUnlock, e)
	e = <-events
	assert.Equal(t, packets.EventPostUnlock, e)
	e = <-events
	assert.Equal(t, packets.EventCompleted, e)

}
