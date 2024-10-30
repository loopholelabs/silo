package protocol

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestProtocolWriteAt(t *testing.T) {
	size := 1024 * 1024
	var store storage.Provider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	pr := NewMockProtocol(context.TODO())

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.Provider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(context.TODO(), 1, storeFactory, pr)

	// Now do some things and make sure they happen...

	go func() {
		_ = destFromProtocol.HandleDevInfo()
	}()
	go func() {
		_ = destFromProtocol.HandleReadAt()
	}()
	go func() {
		_ = destFromProtocol.HandleWriteAt()
	}()

	// Send devInfo
	err := sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	buff := make([]byte, 4096)
	_, err = rand.Read(buff)
	assert.NoError(t, err)
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
	var store storage.Provider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	pr := NewMockProtocol(context.TODO())

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	sourceToProtocol.CompressedWrites = true

	storeFactory := func(di *packets.DevInfo) storage.Provider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(context.TODO(), 1, storeFactory, pr)

	// Now do some things and make sure they happen...

	go func() {
		_ = destFromProtocol.HandleDevInfo()
	}()
	go func() {
		_ = destFromProtocol.HandleReadAt()
	}()
	go func() {
		_ = destFromProtocol.HandleWriteAt()
	}()

	// Send devInfo
	err := sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	buff := make([]byte, 4096)
	_, err = rand.Read(buff)
	assert.NoError(t, err)

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
	var store storage.Provider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	buff := make([]byte, 4096)
	_, err := rand.Read(buff)
	assert.NoError(t, err)

	pr := NewMockProtocol(context.TODO())

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.Provider {
		store = sources.NewMemoryStorage(int(di.Size))

		n, err := store.WriteAt(buff, 12)

		assert.NoError(t, err)
		assert.Equal(t, len(buff), n)

		return store
	}

	destFromProtocol := NewFromProtocol(context.TODO(), 1, storeFactory, pr)

	// Now do some things and make sure they happen...

	go func() {
		_ = destFromProtocol.HandleDevInfo()
	}()
	go func() {
		_ = destFromProtocol.HandleReadAt()
	}()
	go func() {
		_ = destFromProtocol.HandleWriteAt()
	}()

	err = sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err := sourceToProtocol.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}

func TestProtocolRWWriteAt(t *testing.T) {
	size := 1024 * 1024
	var store storage.Provider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	destDev := make(chan uint32, 8)

	storeFactory := func(di *packets.DevInfo) storage.Provider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	prSource := NewRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := NewRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, func(ctx context.Context, p Protocol, dev uint32) {
		destDev <- dev
		destFromProtocol := NewFromProtocol(ctx, dev, storeFactory, p)

		go func() {
			_ = destFromProtocol.HandleDevInfo()
		}()
		go func() {
			_ = destFromProtocol.HandleReadAt()
		}()
		go func() {
			_ = destFromProtocol.HandleWriteAt()
		}()
	})

	sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

	// TODO: Cleanup
	go func() {
		_ = prSource.Handle()
	}()
	go func() {
		_ = prDest.Handle()
	}()

	// Now do some things and make sure they happen...

	err := sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	// Should know the dev now...
	assert.Equal(t, uint32(1), <-destDev)
	assert.Equal(t, 0, len(destDev))

	buff := make([]byte, 4096)
	_, err = rand.Read(buff)
	assert.NoError(t, err)
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
	var store storage.Provider

	buff := make([]byte, 4096)
	_, err := rand.Read(buff)
	assert.NoError(t, err)

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	storeFactory := func(di *packets.DevInfo) storage.Provider {
		store = sources.NewMemoryStorage(int(di.Size))
		n, err := store.WriteAt(buff, 12)

		assert.NoError(t, err)
		assert.Equal(t, len(buff), n)

		return store
	}

	initDev := func(ctx context.Context, p Protocol, dev uint32) {
		destFromProtocol := NewFromProtocol(ctx, dev, storeFactory, p)

		go func() {
			_ = destFromProtocol.HandleDevInfo()
		}()
		go func() {
			_ = destFromProtocol.HandleReadAt()
		}()
		go func() {
			_ = destFromProtocol.HandleWriteAt()
		}()
	}

	prSource := NewRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := NewRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, initDev)

	sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

	// TODO: Cleanup
	go func() {
		_ = prSource.Handle()
	}()
	go func() {
		_ = prDest.Handle()
	}()

	// Now do some things and make sure they happen...
	err = sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	// Now check it was written to the source
	buff2 := make([]byte, 4096)
	n, err := sourceToProtocol.ReadAt(buff2, 12)
	assert.NoError(t, err)
	assert.Equal(t, len(buff2), n)

	assert.Equal(t, buff, buff2)
}

func TestProtocolEvents(t *testing.T) {
	size := 1024 * 1024
	var store storage.Provider

	pr := NewMockProtocol(context.TODO())

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.Provider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(context.TODO(), 1, storeFactory, pr)

	events := make(chan packets.EventType, 10)

	// Now do some things and make sure they happen...
	go func() {
		_ = destFromProtocol.HandleDevInfo()
	}()
	go func() {
		_ = destFromProtocol.HandleEvent(func(e *packets.Event) {
			events <- e.Type
		})
	}()

	// Send devInfo
	err := sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	// Send some events and make sure they happen at the other end...

	err = sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventPreLock})
	assert.NoError(t, err)
	// There should be the event waiting for us already.
	assert.Equal(t, 1, len(events))
	e := <-events
	assert.Equal(t, packets.EventPreLock, e)
	err = sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventPostLock})
	assert.NoError(t, err)
	err = sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventPreUnlock})
	assert.NoError(t, err)
	err = sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventPostUnlock})
	assert.NoError(t, err)
	err = sourceToProtocol.SendEvent(&packets.Event{Type: packets.EventCompleted})
	assert.NoError(t, err)
	e = <-events
	assert.Equal(t, packets.EventPostLock, e)
	e = <-events
	assert.Equal(t, packets.EventPreUnlock, e)
	e = <-events
	assert.Equal(t, packets.EventPostUnlock, e)
	e = <-events
	assert.Equal(t, packets.EventCompleted, e)

}

func TestProtocolWriteAtWithMap(t *testing.T) {
	size := 1024 * 1024
	var store storage.Provider
	var mappedStore *modules.MappedStorage

	pr := NewMockProtocol(context.TODO())

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.Provider {
		store = sources.NewMemoryStorage(int(di.Size))
		mappedStore = modules.NewMappedStorage(store, 4096)
		return store
	}

	destFromProtocol := NewFromProtocol(context.TODO(), 1, storeFactory, pr)

	// Now do some things and make sure they happen...

	go func() {
		_ = destFromProtocol.HandleDevInfo()
	}()
	go func() {
		_ = destFromProtocol.HandleReadAt()
	}()
	go func() {
		_ = destFromProtocol.HandleWriteAt()
	}()

	writeWithMap := func(offset int64, data []byte, idmap map[uint64]uint64) error {
		_, err := store.WriteAt(data, offset)
		assert.NoError(t, err)
		// Update the map
		mappedStore.AppendMap(idmap)
		return nil
	}

	go func() {
		_ = destFromProtocol.HandleWriteAtWithMap(writeWithMap)
	}()

	// Send devInfo
	err := sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	sourceStore := sources.NewMemoryStorage(size)
	sourceMappedStore := modules.NewMappedStorage(sourceStore, 4096)

	buff := make([]byte, 4096)
	// Write a couple of blocks
	_, err = rand.Read(buff)
	assert.NoError(t, err)
	err = sourceMappedStore.WriteBlock(123, buff)
	assert.NoError(t, err)

	_, err = rand.Read(buff)
	assert.NoError(t, err)
	err = sourceMappedStore.WriteBlock(789, buff)
	assert.NoError(t, err)

	// Send it
	idmap := sourceMappedStore.GetMapForSourceRange(0, len(buff)*2)
	senddata := make([]byte, len(buff)*2)
	_, err = sourceStore.ReadAt(senddata, 0)
	assert.NoError(t, err)

	n, err := sourceToProtocol.WriteAtWithMap(senddata, 0, idmap)
	assert.NoError(t, err)
	assert.Equal(t, len(senddata), n)

	// Now check it was written to the source in the right place etc...
	for _, id := range []uint64{123, 789} {
		bufferLocal := make([]byte, 4096)
		bufferRemote := make([]byte, 4096)
		err = mappedStore.ReadBlock(id, bufferLocal)
		assert.NoError(t, err)
		err = mappedStore.ReadBlock(id, bufferRemote)
		assert.NoError(t, err)
		assert.Equal(t, bufferLocal, bufferRemote)
	}
}
