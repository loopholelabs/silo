package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
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
	newdevProtocol Protocol
}

func NewProtocolRW(ctx context.Context, readers []io.Reader, writers []io.Writer, newdevFN func(Protocol, uint32)) *ProtocolRW {
	prw := &ProtocolRW{
		ctx:        ctx,
		waiters:    make(map[uint32]Waiters),
		newdevFN:   newdevFN,
		activeDevs: make(map[uint32]bool),
	}

	prw.readers = readers

	prw.writers = writers
	prw.writerLocks = make([]sync.Mutex, len(writers))
	prw.writerHeaders = make([][]byte, len(writers))
	for i := 0; i < len(writers); i++ {
		prw.writerHeaders[i] = make([]byte, 4+4+4)
	}
	prw.newdevProtocol = prw // Return ourselves by default.
	return prw
}

func (p *ProtocolRW) SetNewDevProtocol(proto Protocol) {
	p.newdevProtocol = proto
}

func (p *ProtocolRW) initDev(dev uint32) {
	p.activeDevsLock.Lock()
	_, ok := p.activeDevs[dev]
	if !ok {
		p.activeDevs[dev] = true

		// Setup waiter here, this is the first packet we've received for this dev.
		p.waitersLock.Lock()
		p.waiters[dev] = Waiters{
			byCmd: make(map[byte]chan packetinfo),
			byID:  make(map[uint32]chan packetinfo),
		}
		p.waitersLock.Unlock()

		if p.newdevFN != nil {
			p.newdevFN(p.newdevProtocol, dev)
		}
	}
	p.activeDevsLock.Unlock()
}

func (p *ProtocolRW) SendPacketWriter(dev uint32, id uint32, length uint32, data func(w io.Writer) error) (uint32, error) {
	// If the context was cancelled, we should return that error
	select {
	case <-p.ctx.Done():
		return 0, p.ctx.Err()
	default:
		break
	}

	p.initDev(dev)

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
	// If the context was cancelled, we should return that error
	select {
	case <-p.ctx.Done():
		return 0, p.ctx.Err()
	default:
		break
	}

	p.initDev(dev)

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
	errs := make(chan error, len(p.readers))

	for _, r := range p.readers {
		go func(reader io.Reader) {
			header := make([]byte, 4+4+4)
			for {
				// If the context was cancelled, we should return that error
				select {
				case <-p.ctx.Done():
					errs <- p.ctx.Err()
					return
				default:
					break
				}

				_, err := io.ReadFull(reader, header)
				if err != nil {
					errs <- err
					return
				}
				dev := binary.LittleEndian.Uint32(header)
				id := binary.LittleEndian.Uint32(header[4:])
				length := binary.LittleEndian.Uint32(header[8:])

				data := make([]byte, length)
				_, err = io.ReadFull(reader, data)

				if err != nil {
					errs <- err
					return
				}

				err = p.handlePacket(dev, id, data)
				if err != nil {
					errs <- err
					return
				}
			}
		}(r)
	}

	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case e := <-errs:
		// One of the readers quit. We should report the error...
		return e
	}
}

func (p *ProtocolRW) handlePacket(dev uint32, id uint32, data []byte) error {
	p.initDev(dev)

	if data == nil || len(data) < 1 {
		return errors.New("Invalid data packet")
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
	return nil
}

func (mp *ProtocolRW) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	mp.waitersLock.Lock()
	w := mp.waiters[dev]
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
	w := mp.waiters[dev]
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
