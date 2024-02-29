package protocol

import (
	"context"
	"encoding/binary"
	"io"
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

type ProtocolRW struct {
	ctx          context.Context
	r            io.Reader
	w            io.Writer
	w_lock       sync.Mutex
	tx_id        uint32
	active_devs  map[uint32]bool
	waiters      map[uint32]Waiters
	waiters_lock sync.Mutex
	newdev_fn    func(Protocol, uint32)
}

func NewProtocolRW(ctx context.Context, r io.Reader, w io.Writer, newdev_fn func(Protocol, uint32)) *ProtocolRW {
	return &ProtocolRW{
		ctx:         ctx,
		r:           r,
		w:           w,
		waiters:     make(map[uint32]Waiters),
		newdev_fn:   newdev_fn,
		active_devs: make(map[uint32]bool),
	}
}

// Send a packet
func (p *ProtocolRW) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	// Encode and send it down the writer...
	if id == ID_PICK_ANY {
		id = atomic.AddUint32(&p.tx_id, 1)
	}

	buffer := make([]byte, 4+4+4)
	binary.LittleEndian.PutUint32(buffer, dev)
	binary.LittleEndian.PutUint32(buffer[4:], id)
	binary.LittleEndian.PutUint32(buffer[8:], uint32(len(data)))
	p.w_lock.Lock()
	defer p.w_lock.Unlock()

	_, err := p.w.Write(buffer)

	if err != nil {
		return 0, err
	}
	_, err = p.w.Write(data)
	return id, err
}

// Read a packet
func (p *ProtocolRW) readPacket() (uint32, uint32, []byte, error) {
	buffer := make([]byte, 4+4+4)

	_, err := io.ReadFull(p.r, buffer)
	if err != nil {
		return 0, 0, nil, err
	}
	dev := binary.LittleEndian.Uint32(buffer)
	id := binary.LittleEndian.Uint32(buffer[4:])
	length := binary.LittleEndian.Uint32(buffer[8:])

	data := make([]byte, length)

	_, err = io.ReadFull(p.r, data)

	if err != nil {
		return 0, 0, nil, err
	}
	return dev, id, data, nil
}

func (p *ProtocolRW) Handle() error {
	for {
		dev, id, data, err := p.readPacket()
		if err != nil {
			return err
		}
		// Now queue it up in a channel

		_, ok := p.active_devs[dev]
		if !ok {
			p.active_devs[dev] = true
			if p.newdev_fn != nil {
				p.newdev_fn(p, dev)
			}
		}

		cmd := data[0]

		p.waiters_lock.Lock()
		w, ok := p.waiters[dev]
		if !ok {
			w = Waiters{
				by_cmd: make(map[byte]chan packetinfo),
				by_id:  make(map[uint32]chan packetinfo),
			}
			p.waiters[dev] = w
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

		p.waiters_lock.Unlock()

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
	}
}

func (mp *ProtocolRW) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
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

	select {
	case p := <-wq:
		// TODO: Remove the channel, as we only expect a SINGLE response with this ID.
		return p.data, nil
	case <-mp.ctx.Done():
		return nil, mp.ctx.Err()
	}
}

func (mp *ProtocolRW) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
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

	select {
	case p := <-wq:
		return p.id, p.data, nil
	case <-mp.ctx.Done():
		return 0, nil, mp.ctx.Err()
	}
}
