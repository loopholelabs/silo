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
	ctx          context.Context
	waiters      map[uint32]Waiters
	waiters_lock sync.Mutex
	tid          uint32
}

func NewMockProtocol(ctx context.Context) *MockProtocol {
	return &MockProtocol{
		ctx:     ctx,
		waiters: make(map[uint32]Waiters),
		tid:     0,
	}
}

func (mp *MockProtocol) SendPacketWriter(dev uint32, id uint32, length uint32, header []byte, data func(io.Writer) error) (uint32, error) {
	var buff bytes.Buffer
	buff.Write(header)
	err := data(&buff)
	if err != nil {
		return 0, err
	}

	return mp.SendPacket(dev, id, buff.Bytes())
}

func (mp *MockProtocol) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	cmd := data[0]

	// if id is ANY, pick one
	if id == ID_PICK_ANY {
		id = atomic.AddUint32(&mp.tid, 1)
		// TODO. If id wraps around and becomes ID_PICK_ANY etc
	}

	mp.waiters_lock.Lock()
	w, ok := mp.waiters[dev]
	if !ok {
		w = Waiters{
			byCmd: make(map[byte]chan packetinfo),
			byID:  make(map[uint32]chan packetinfo),
		}
		mp.waiters[dev] = w
	}

	wq_id, okk := w.byID[id]
	if !okk {
		wq_id = make(chan packetinfo, 8) // Some buffer here...
		w.byID[id] = wq_id
	}

	wq_cmd, okk := w.byCmd[cmd]
	if !okk {
		wq_cmd = make(chan packetinfo, 8) // Some buffer here...
		w.byCmd[cmd] = wq_cmd
	}

	mp.waiters_lock.Unlock()

	// Send it to any listeners
	// If this matches something being waited for, route it there.
	// TODO: Don't always do this, expire, etc etc

	if packets.IsResponse(cmd) {
		wq_id <- packetinfo{
			id:   id,
			data: data,
		}
	} else {
		wq_cmd <- packetinfo{
			id:   id,
			data: data,
		}
	}

	return id, nil
}

func (mp *MockProtocol) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	mp.waiters_lock.Lock()
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
	mp.waiters_lock.Unlock()

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
	mp.waiters_lock.Lock()
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
	mp.waiters_lock.Unlock()

	// Wait for the packet to appear on the channel
	select {
	case p := <-wq:
		return p.id, p.data, nil
	case <-mp.ctx.Done():
		return 0, nil, mp.ctx.Err()
	}
}
