package protocol

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestTestProtocolLatency(t *testing.T) {

	size := 1024 * 1024
	var store storage.StorageProvider

	// Setup a protocol in the middle, and make sure our reads/writes get through ok

	prm := NewMockProtocol()
	// Add some recv latency
	pr := NewTestProtocolLatency(prm, 50*time.Millisecond)

	sourceToProtocol := NewToProtocol(uint64(size), 1, pr)

	storeFactory := func(di *packets.DevInfo) storage.StorageProvider {
		store = sources.NewMemoryStorage(int(di.Size))
		return store
	}

	destFromProtocol := NewFromProtocol(1, storeFactory, pr)

	go destFromProtocol.HandleDevInfo()
	go destFromProtocol.HandleReadAt()
	go destFromProtocol.HandleWriteAt()

	ctime := time.Now()

	// Send devInfo
	sourceToProtocol.SendDevInfo("test", 4096)

	buff := make([]byte, 4096)
	rand.Read(buff)
	n, err := sourceToProtocol.WriteAt(buff, 12)

	assert.NoError(t, err)
	assert.Equal(t, len(buff), n)

	// Check it took some time
	assert.WithinDuration(t, ctime.Add(50*time.Millisecond), time.Now(), 10*time.Millisecond)
}
