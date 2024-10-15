package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestProtocolLogger(t *testing.T) {
	size := 1024 * 1024
	var store storage.StorageProvider

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

	var logger_dest *Protocol_logger

	prSource := NewProtocolRW(ctx, []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := NewProtocolRW(ctx, []io.Reader{r2}, []io.Writer{w1}, func(ctx context.Context, p Protocol, dev uint32) {
		destDev <- dev
		logger_dest = NewProtocolLogger(fmt.Sprintf("dest%d", dev), p)
		destFromProtocol := NewFromProtocol(ctx, dev, storeFactory, logger_dest)

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

	logger_source := NewProtocolLogger("source", prSource)
	sourceToProtocol := NewToProtocol(uint64(size), 1, logger_source)

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

	// Do a WriteAt and a ReadAt
	buffer := make([]byte, 4096)
	_, err = sourceToProtocol.WriteAt(buffer, 0)
	assert.NoError(t, err)

	_, err = sourceToProtocol.ReadAt(buffer, 0)
	assert.NoError(t, err)

	cancelFn()

	wg.Wait()

	metrics_protocol_source := logger_source.Metrics()
	metrics_protocol_dest := logger_dest.Metrics()

	assert.Equal(t, int64(0), metrics_protocol_source.Pending_send_packets)
	assert.Equal(t, int64(0), metrics_protocol_source.Pending_send_packet_writers)
	assert.Equal(t, int64(0), metrics_protocol_source.Pending_wait_for_packets)

	assert.Equal(t, int64(0), metrics_protocol_dest.Pending_send_packets)
	assert.Equal(t, int64(0), metrics_protocol_dest.Pending_send_packet_writers)
	assert.Equal(t, int64(0), metrics_protocol_dest.Pending_wait_for_packets)

	fmt.Printf("SOURCE %v\n", metrics_protocol_source)
	fmt.Printf("DEST %v\n", metrics_protocol_dest)
}
