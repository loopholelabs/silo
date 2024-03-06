package protocol

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
)

type MockProtocol struct {
	waiters     map[uint32]Waiters
	waitersLock sync.Mutex
	tid         uint32
}

func NewMockProtocol() *MockProtocol {
	return &MockProtocol{
		waiters: make(map[uint32]Waiters),
		tid:     0,
	}
}

func (mp *MockProtocol) SendPacketWriter(dev uint32, id uint32, length uint32, data func(io.Writer) error) (uint32, error) {
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
	if id == ID_PICK_ANY {
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

	mp.waitersLock.Unlock()

	// Send it to any listeners
	// If this matches something being waited for, route it there.
	// TODO: Don't always do this, expire, etc etc

	if IsResponse(cmd) {
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
	p := <-wq

	// TODO: Could remove the channel now idk... we'll see...

	return p.data, nil
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
	p := <-wq
	return p.id, p.data, nil
}
