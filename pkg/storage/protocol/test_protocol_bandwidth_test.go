package protocol

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestTestProtocolBandwidth(t *testing.T) {

	size := 1024 * 1024
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	prm := NewMockProtocol(context.TODO())
	// Add some recv latency
	pr := NewTestProtocolBandwidth(prm, 100*1024) // allow 100KB/sec

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(context.TODO(), 1, storeFactory, pr)

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
	if err != nil {
		panic(err)
	}

	// We'll send around 100KB through, and make sure it takes around a second...

	buff := make([]byte, 1024)
	_, err = rand.Read(buff)
	if err != nil {
		panic(err)
	}
	ctime := time.Now()

	for n := 0; n < 100; n++ {
		n, err := sourceToProtocol.WriteAt(buff, 12)
		assert.NoError(t, err)
		assert.Equal(t, len(buff), n)
	}

	// Check it took some time...
	assert.WithinDuration(t, ctime.Add(1*time.Second), time.Now(), 100*time.Millisecond)
}
