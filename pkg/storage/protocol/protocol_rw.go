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
	byCmd map[byte]chan packetinfo
	byID  map[uint32]chan packetinfo
}

type ProtocolRW struct {
	ctx         context.Context
	r           io.Reader
	w           io.Writer
	wLock       sync.Mutex
	wHeader     []byte
	txID        uint32
	activeDevs  map[uint32]bool
	waiters     map[uint32]Waiters
	waitersLock sync.Mutex
	newdevFN    func(Protocol, uint32)
}

func NewProtocolRW(ctx context.Context, r io.Reader, w io.Writer, newdevFN func(Protocol, uint32)) *ProtocolRW {
	return &ProtocolRW{
		ctx:        ctx,
		r:          r,
		w:          w,
		waiters:    make(map[uint32]Waiters),
		newdevFN:   newdevFN,
		activeDevs: make(map[uint32]bool),
		wHeader:    make([]byte, 12),
	}
}

// Send a packet
func (p *ProtocolRW) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	// Encode and send it down the writer...
	if id == ID_PICK_ANY {
		id = atomic.AddUint32(&p.txID, 1)
	}

	p.wLock.Lock()
	defer p.wLock.Unlock()

	binary.LittleEndian.PutUint32(p.wHeader, dev)
	binary.LittleEndian.PutUint32(p.wHeader[4:], id)
	binary.LittleEndian.PutUint32(p.wHeader[8:], uint32(len(data)))
	_, err := p.w.Write(p.wHeader)

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

		_, ok := p.activeDevs[dev]
		if !ok {
			p.activeDevs[dev] = true
			if p.newdevFN != nil {
				p.newdevFN(p, dev)
			}
		}

		cmd := data[0]

		p.waitersLock.Lock()
		w, ok := p.waiters[dev]
		if !ok {
			w = Waiters{
				byCmd: make(map[byte]chan packetinfo),
				byID:  make(map[uint32]chan packetinfo),
			}
			p.waiters[dev] = w
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

		p.waitersLock.Unlock()

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

	select {
	case p := <-wq:
		// TODO: Remove the channel, as we only expect a SINGLE response with this ID.
		return p.data, nil
	case <-mp.ctx.Done():
		return nil, mp.ctx.Err()
	}
}

func (mp *ProtocolRW) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
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

	select {
	case p := <-wq:
		return p.id, p.data, nil
	case <-mp.ctx.Done():
		return 0, nil, mp.ctx.Err()
	}
}
