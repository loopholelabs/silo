package protocol

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestTestProtocolBandwidth(t *testing.T) {

	size := 1024 * 1024
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	prm := NewMockProtocol()
	// Add some recv latency
	pr := NewTestProtocolBandwidth(prm, 100*1024) // allow 100KB/sec

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(1, storeFactory, pr)

	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleSend(context.TODO())
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	// Send devInfo
	sourceToProtocol.SendDevInfo("test", 4096)

	// We'll send around 100KB through, and make sure it takes around a second...

	buff := make([]byte, 1024)
	rand.Read(buff)
	ctime := time.Now()

	for n := 0; n < 100; n++ {
		n, err := sourceToProtocol.WriteAt(buff, 12)
		assert.NoError(t, err)
		assert.Equal(t, len(buff), n)
	}

	// Check it took some time...
	assert.WithinDuration(t, ctime.Add(1*time.Second), time.Now(), 100*time.Millisecond)
}