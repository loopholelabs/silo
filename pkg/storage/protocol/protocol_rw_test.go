package protocol

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestProtocolRWCancel(t *testing.T) {
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

	ctx, cancelFn := context.WithCancel(context.TODO())

	var wg sync.WaitGroup

	prSource := NewProtocolRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := NewProtocolRW(ctx, []io.Reader{r2}, []io.Writer{w1}, func(p Protocol, dev uint32) {
		destDev <- dev
		destFromProtocol := NewFromProtocol(dev, storeFactory, p)

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

	wg.Add(1)
	go func() {
		err := prSource.Handle()
		assert.True(t, errors.Is(err, context.Canceled))
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		err := prDest.Handle()
		assert.True(t, errors.Is(err, context.Canceled))
		wg.Done()
	}()

	// Now do some things and make sure they happen...

	err := sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	// Should know the dev now...
	assert.Equal(t, uint32(1), <-destDev)
	assert.Equal(t, 0, len(destDev))
	/*
		// Close the connections
		w1.Close()
		r1.Close()
		w2.Close()
		r2.Close()
	*/
	// Now cancel context, and make sure things get cleaned up
	cancelFn()

	wg.Wait()
}

func TestProtocolRWCancelFromHandler(t *testing.T) {
	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	ctx, cancelFn := context.WithCancel(context.TODO())

	var wg sync.WaitGroup

	prSource := NewProtocolRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := NewProtocolRW(ctx, []io.Reader{r2}, []io.Writer{w1}, func(p Protocol, dev uint32) {
		cancelFn()
	})

	sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

	wg.Add(1)
	go func() {
		err := prSource.Handle()
		assert.True(t, errors.Is(err, context.Canceled))
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		err := prDest.Handle()
		assert.True(t, errors.Is(err, context.Canceled))
		wg.Done()
	}()

	// Now do some things and make sure they happen...

	err := sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)

	/*
		// Close the connections
		w1.Close()
		r1.Close()
		w2.Close()
		r2.Close()
	*/
	wg.Wait()
}

func TestProtocolRWSendAfterCancel(t *testing.T) {
	// Setup a couple of pipes that would simulate network for example
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	ctx, cancelFn := context.WithCancel(context.TODO())

	var wg sync.WaitGroup

	prSource := NewProtocolRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := NewProtocolRW(ctx, []io.Reader{r2}, []io.Writer{w1}, func(p Protocol, dev uint32) {
		cancelFn()
	})

	sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

	wg.Add(1)
	go func() {
		err := prSource.Handle()
		assert.True(t, errors.Is(err, context.Canceled))
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		err := prDest.Handle()
		assert.True(t, errors.Is(err, context.Canceled))
		wg.Done()
	}()

	// Now do some things and make sure they happen...

	err := sourceToProtocol.SendDevInfo("test", 4096, "")
	assert.NoError(t, err)
	wg.Wait()

	// Now check that we can't send anything...

	_, err = prDest.SendPacket(1, 0, []byte{1, 2, 3})
	assert.ErrorIs(t, err, context.Canceled)

	_, err = prDest.SendPacketWriter(1, 0, 0, func(w io.Writer) error { return nil })
	assert.ErrorIs(t, err, context.Canceled)

}
