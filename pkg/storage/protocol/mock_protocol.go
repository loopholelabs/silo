package protocol

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

type MockProtocol struct {
	ctx         context.Context
	waiters     map[uint32]Waiters
	waitersLock sync.Mutex
	tid         uint32
}

func NewMockProtocol(ctx context.Context) *MockProtocol {
	return &MockProtocol{
		ctx:     ctx,
		waiters: make(map[uint32]Waiters),
		tid:     0,
	}
}

func (mp *MockProtocol) SendPacketWriter(dev uint32, id uint32, _ uint32, data func(io.Writer) error) (uint32, error) {
	var buff bytes.Buffer
	err := data(&buff)
	if err != nil {
		return 0, err
	}

	return mp.SendPacket(dev, id, buff.Bytes())
}

func (mp *MockProtocol) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	cmd := data[0]

	// if id is ANY, pick one
	if id == IDPickAny {
		id = atomic.AddUint32(&mp.tid, 1)
		// TODO. If id wraps around and becomes ID_PICK_ANY etc
	}

	mp.waitersLock.Lock()
	w, ok := mp.waiters[dev]
	if !ok {
		w = Waiters{
			byCmd: make(map[byte]chan packetinfo),
			byID:  make(map[uint32]chan packetinfo),
		}
		mp.waiters[dev] = w
	}

	wqID, okk := w.byID[id]
	if !okk {
		wqID = make(chan packetinfo, 8) // Some buffer here...
		w.byID[id] = wqID
	}

	wqCmd, okk := w.byCmd[cmd]
	if !okk {
		wqCmd = make(chan packetinfo, 8) // Some buffer here...
		w.byCmd[cmd] = wqCmd
	}

	mp.waitersLock.Unlock()

	// Send it to any listeners
	// If this matches something being waited for, route it there.
	// TODO: Don't always do this, expire, etc etc

	if packets.IsResponse(cmd) {
		wqID <- packetinfo{
			id:   id,
			data: data,
		}
	} else {
		wqCmd <- packetinfo{
			id:   id,
			data: data,
		}
	}

	return id, nil
}

func (mp *MockProtocol) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	mp.waitersLock.Lock()
	w, ok := mp.waiters[dev]
	if !ok {
		w = Waiters{
			byCmd: make(map[byte]chan packetinfo),
			byID:  make(map[uint32]chan packetinfo),
		}
		mp.waiters[dev] = w
	}
	wq, okk := w.byID[id]
	if !okk {
		wq = make(chan packetinfo, 8) // Some buffer here...
		w.byID[id] = wq
	}
	mp.waitersLock.Unlock()

	// Wait for the packet to appear on the channel
	select {
	case p := <-wq:
		return p.data, nil
	case <-mp.ctx.Done():
		return nil, mp.ctx.Err()
	}
	// TODO: Could remove the channel now idk... we'll see...

}

func (mp *MockProtocol) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	mp.waitersLock.Lock()
	w, ok := mp.waiters[dev]
	if !ok {
		w = Waiters{
			byCmd: make(map[byte]chan packetinfo),
			byID:  make(map[uint32]chan packetinfo),
		}
		mp.waiters[dev] = w
	}
	wq, okk := w.byCmd[cmd]
	if !okk {
		wq = make(chan packetinfo, 8) // Some buffer here...
		w.byCmd[cmd] = wq
	}
	mp.waitersLock.Unlock()

	// Wait for the packet to appear on the channel
	select {
	case p := <-wq:
		return p.id, p.data, nil
	case <-mp.ctx.Done():
		return 0, nil, mp.ctx.Err()
	}
}
