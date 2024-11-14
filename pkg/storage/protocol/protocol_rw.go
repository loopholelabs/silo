package protocol

import (
	"context"
	"encoding/binary"
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

type RW struct {
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
	newdevFn       func(context.Context, Protocol, uint32)
	newdevProtocol Protocol
}

func NewRW(ctx context.Context, readers []io.Reader, writers []io.Writer, newdevFN func(context.Context, Protocol, uint32)) *RW {
	return NewRWWithBuffering(ctx, readers, writers, nil, newdevFN)
}

func NewRWWithBuffering(ctx context.Context, readers []io.Reader, writers []io.Writer, bufferConfig *BufferedWriterConfig, newdevFN func(context.Context, Protocol, uint32)) *RW {
	prw := &RW{
		ctx:        ctx,
		waiters:    make(map[uint32]Waiters),
		newdevFn:   newdevFN,
		activeDevs: make(map[uint32]bool),
	}

	prw.readers = readers

	prw.writers = make([]io.Writer, 0)
	for _, w := range writers {
		if bufferConfig != nil {
			prw.writers = append(prw.writers, NewBufferedWriter(w, bufferConfig))
		} else {
			prw.writers = append(prw.writers, w)
		}
	}

	prw.writerLocks = make([]sync.Mutex, len(writers))
	prw.writerHeaders = make([][]byte, len(writers))
	for i := 0; i < len(writers); i++ {
		prw.writerHeaders[i] = make([]byte, 4+4+4)
	}
	prw.newdevProtocol = prw // Return ourselves by default.
	return prw
}

func (p *RW) SetNewDevProtocol(proto Protocol) {
	p.newdevProtocol = proto
}

func (p *RW) InitDev(dev uint32) {
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

		if p.newdevFn != nil {
			p.newdevFn(p.ctx, p.newdevProtocol, dev)
		}
	}
	p.activeDevsLock.Unlock()
}

// Send a packet
func (p *RW) SendPacket(dev uint32, id uint32, data []byte, urgency Urgency) (uint32, error) {
	// If the context was cancelled, we should return that error
	select {
	case <-p.ctx.Done():
		return 0, p.ctx.Err()
	default:
		break
	}

	p.InitDev(dev)

	// Encode and send it down the writer...
	if id == IDPickAny {
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

	// If it's urgent, then use WriteNow if we can
	wwu, ok := p.writers[i].(WriterWithUrgent)
	if urgency == UrgencyUrgent && ok {
		_, err = wwu.WriteNow(data)
	} else {
		_, err = p.writers[i].Write(data)
	}

	return id, err
}

func (p *RW) Handle() error {
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

func (p *RW) handlePacket(dev uint32, id uint32, data []byte) error {
	p.InitDev(dev)

	if data == nil || len(data) < 1 {
		return packets.ErrInvalidPacket
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

	p.waitersLock.Unlock()

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
	return nil
}

func (p *RW) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	p.waitersLock.Lock()
	w := p.waiters[dev]
	wq, okk := w.byID[id]
	if !okk {
		wq = make(chan packetinfo, 8) // Some buffer here...
		w.byID[id] = wq
	}
	p.waitersLock.Unlock()

	select {
	case p := <-wq:
		// TODO: Remove the channel, as we only expect a SINGLE response with this ID.
		return p.data, nil
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
}

func (p *RW) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	p.waitersLock.Lock()
	w := p.waiters[dev]
	wq, okk := w.byCmd[cmd]
	if !okk {
		wq = make(chan packetinfo, 8) // Some buffer here...
		w.byCmd[cmd] = wq
	}
	p.waitersLock.Unlock()

	select {
	case p := <-wq:
		return p.id, p.data, nil
	case <-p.ctx.Done():
		return 0, nil, p.ctx.Err()
	}
}
