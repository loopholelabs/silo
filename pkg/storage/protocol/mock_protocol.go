package protocol

import (
	"sync"
	"sync/atomic"
)

type packetinfo struct {
	id   uint32
	data []byte
}

type Waiters struct {
	by_cmd map[byte]chan packetinfo
	by_id  map[uint32]chan packetinfo
}

type MockProtocol struct {
	waiters      map[uint32]Waiters
	waiters_lock sync.Mutex
	tid          uint32
}

func NewMockProtocol() *MockProtocol {
	return &MockProtocol{
		waiters: make(map[uint32]Waiters),
		tid:     0,
	}
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
			by_cmd: make(map[byte]chan packetinfo),
			by_id:  make(map[uint32]chan packetinfo),
		}
		mp.waiters[dev] = w
	}

	wq_id, okk := w.by_id[id]
	if !okk {
		wq_id = make(chan packetinfo, 8) // Some buffer here...
		w.by_id[id] = wq_id
	}

	wq_cmd, okk := w.by_cmd[cmd]
	if !okk {
		wq_cmd = make(chan packetinfo, 8) // Some buffer here...
		w.by_cmd[cmd] = wq_cmd
	}

	mp.waiters_lock.Unlock()

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
	mp.waiters_lock.Lock()
	w, ok := mp.waiters[dev]
	if !ok {
		w = Waiters{
			by_cmd: make(map[byte]chan packetinfo),
			by_id:  make(map[uint32]chan packetinfo),
		}
		mp.waiters[dev] = w
	}
	wq, okk := w.by_id[id]
	if !okk {
		wq = make(chan packetinfo, 8) // Some buffer here...
		w.by_id[id] = wq
	}
	mp.waiters_lock.Unlock()

	// Wait for the packet to appear on the channel
	p := <-wq

	// TODO: Could remove the channel now idk... we'll see...

	return p.data, nil
}

func (mp *MockProtocol) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	mp.waiters_lock.Lock()
	w, ok := mp.waiters[dev]
	if !ok {
		w = Waiters{
			by_cmd: make(map[byte]chan packetinfo),
			by_id:  make(map[uint32]chan packetinfo),
		}
		mp.waiters[dev] = w
	}
	wq, okk := w.by_cmd[cmd]
	if !okk {
		wq = make(chan packetinfo, 8) // Some buffer here...
		w.by_cmd[cmd] = wq
	}
	mp.waiters_lock.Unlock()

	// Wait for the packet to appear on the channel
	p := <-wq
	return p.id, p.data, nil
}
