package protocol

import (
	"context"
	"encoding/binary"
	"io"
	"math/rand"
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
	ctx            context.Context
	readers        []io.Reader
	writers        []io.Writer
	writerHeaders  [][]byte
	writerLocks    []sync.Mutex
	txID           uint32
	activeDevs     map[uint32]bool
	activeDevsLock sync.Mutex
	waiters        map[uint32]Waiters
	waitersLock    sync.Mutex
	newdevFN       func(Protocol, uint32)
}

func NewProtocolRW(ctx context.Context, r io.Reader, writers []io.Writer, newdevFN func(Protocol, uint32)) *ProtocolRW {
	prw := &ProtocolRW{
		ctx:        ctx,
		waiters:    make(map[uint32]Waiters),
		newdevFN:   newdevFN,
		activeDevs: make(map[uint32]bool),
	}

	prw.readers = []io.Reader{r}

	prw.writers = writers
	prw.writerLocks = make([]sync.Mutex, len(writers))
	prw.writerHeaders = make([][]byte, len(writers))
	for i := 0; i < len(writers); i++ {
		prw.writerHeaders[i] = make([]byte, 4+4+4)
	}
	return prw
}

func (p *ProtocolRW) SendPacketWriter(dev uint32, id uint32, length uint32, data func(w io.Writer) error) (uint32, error) {
	// Encode and send it down the writer...
	if id == ID_PICK_ANY {
		id = atomic.AddUint32(&p.txID, 1)
	}

	i := rand.Intn(len(p.writers))

	p.writerLocks[i].Lock()
	defer p.writerLocks[i].Unlock()

	binary.LittleEndian.PutUint32(p.writerHeaders[i], dev)
	binary.LittleEndian.PutUint32(p.writerHeaders[i][4:], id)
	binary.LittleEndian.PutUint32(p.writerHeaders[i][8:], length)

	_, err := p.writers[i].Write(p.writerHeaders[i])

	if err != nil {
		return 0, err
	}
	return id, data(p.writers[i])
}

// Send a packet
func (p *ProtocolRW) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	// Encode and send it down the writer...
	if id == ID_PICK_ANY {
		id = atomic.AddUint32(&p.txID, 1)
	}

	i := rand.Intn(len(p.writers))

	p.writerLocks[i].Lock()
	defer p.writerLocks[i].Unlock()

	binary.LittleEndian.PutUint32(p.writerHeaders[i], dev)
	binary.LittleEndian.PutUint32(p.writerHeaders[i][4:], id)
	binary.LittleEndian.PutUint32(p.writerHeaders[i][8:], uint32(len(data)))

	_, err := p.writers[i].Write(p.writerHeaders[i])

	if err != nil {
		return 0, err
	}
	_, err = p.writers[i].Write(data)
	return id, err
}

func (p *ProtocolRW) Handle() error {
	r := p.readers[0]
	header := make([]byte, 4+4+4)
	for {
		_, err := io.ReadFull(r, header)
		if err != nil {
			return err
		}
		dev := binary.LittleEndian.Uint32(header)
		id := binary.LittleEndian.Uint32(header[4:])
		length := binary.LittleEndian.Uint32(header[8:])

		data := make([]byte, length)
		_, err = io.ReadFull(r, data)

		if err != nil {
			return err
		}

		// Handle the packet
		p.handlePacket(dev, id, data)
	}
}

func (p *ProtocolRW) handlePacket(dev uint32, id uint32, data []byte) {
	p.activeDevsLock.Lock()
	_, ok := p.activeDevs[dev]
	if !ok {
		p.activeDevs[dev] = true
		if p.newdevFN != nil {
			p.newdevFN(p, dev)
		}
	}
	p.activeDevsLock.Unlock()

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
