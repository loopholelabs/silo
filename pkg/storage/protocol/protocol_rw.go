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
	ctx              context.Context
	readers          []io.Reader
	writers          []io.Writer
	writer_headers   [][]byte
	writer_locks     []sync.Mutex
	txID             uint32
	active_devs      map[uint32]bool
	active_devs_lock sync.Mutex
	waiters          map[uint32]Waiters
	waiters_lock     sync.Mutex
	newdev_fn        func(context.Context, Protocol, uint32)
	newdev_protocol  Protocol

	Metric_recv_bytes   uint64
	Metric_recv_packets uint64
	Metric_sent_bytes   uint64
	Metric_sent_packets uint64

	metric_sent_packets_lock    sync.Mutex
	metric_sent_packets_by_type map[byte]uint64
	metric_recv_packets_lock    sync.Mutex
	metric_recv_packets_by_type map[byte]uint64
}

func NewProtocolRW(ctx context.Context, readers []io.Reader, writers []io.Writer, newdevFN func(context.Context, Protocol, uint32)) *ProtocolRW {
	prw := &ProtocolRW{
		ctx:         ctx,
		waiters:     make(map[uint32]Waiters),
		newdev_fn:   newdevFN,
		active_devs: make(map[uint32]bool),
	}

	prw.readers = readers

	prw.writers = writers
	prw.writer_locks = make([]sync.Mutex, len(writers))
	prw.writer_headers = make([][]byte, len(writers))
	for i := 0; i < len(writers); i++ {
		prw.writer_headers[i] = make([]byte, 4+4+4)
	}
	prw.newdev_protocol = prw // Return ourselves by default.

	prw.metric_sent_packets_by_type = make(map[byte]uint64)
	prw.metric_recv_packets_by_type = make(map[byte]uint64)
	return prw
}

type ProtocolRWMetrics struct {
	Recv_bytes          uint64
	Recv_packets        uint64
	Recv_packets_by_cmd map[byte]uint64
	Sent_bytes          uint64
	Sent_packets        uint64
	Sent_packets_by_cmd map[byte]uint64
}

func (p *ProtocolRW) Metrics() *ProtocolRWMetrics {
	r_by_cmd := make(map[byte]uint64)
	p.metric_recv_packets_lock.Lock()
	for cmd, count := range p.metric_recv_packets_by_type {
		r_by_cmd[cmd] = count
	}
	p.metric_recv_packets_lock.Unlock()
	s_by_cmd := make(map[byte]uint64)
	p.metric_sent_packets_lock.Lock()
	for cmd, count := range p.metric_sent_packets_by_type {
		s_by_cmd[cmd] = count
	}
	p.metric_recv_packets_lock.Unlock()
	return &ProtocolRWMetrics{
		Recv_bytes:          atomic.LoadUint64(&p.Metric_recv_bytes),
		Recv_packets:        atomic.LoadUint64(&p.Metric_recv_packets),
		Recv_packets_by_cmd: r_by_cmd,
		Sent_bytes:          atomic.LoadUint64(&p.Metric_sent_bytes),
		Sent_packets:        atomic.LoadUint64(&p.Metric_sent_packets),
		Sent_packets_by_cmd: s_by_cmd,
	}
}

func (p *ProtocolRW) SetNewDevProtocol(proto Protocol) {
	p.newdev_protocol = proto
}

func (p *ProtocolRW) InitDev(dev uint32) {
	p.active_devs_lock.Lock()
	_, ok := p.active_devs[dev]
	if !ok {
		p.active_devs[dev] = true

		// Setup waiter here, this is the first packet we've received for this dev.
		p.waiters_lock.Lock()
		p.waiters[dev] = Waiters{
			byCmd: make(map[byte]chan packetinfo),
			byID:  make(map[uint32]chan packetinfo),
		}
		p.waiters_lock.Unlock()

		if p.newdev_fn != nil {
			p.newdev_fn(p.ctx, p.newdev_protocol, dev)
		}
	}
	p.active_devs_lock.Unlock()
}

func (p *ProtocolRW) SendPacketWriter(dev uint32, id uint32, length uint32, header []byte, data func(w io.Writer) error) (uint32, error) {
	// If the context was cancelled, we should return that error
	select {
	case <-p.ctx.Done():
		return 0, p.ctx.Err()
	default:
		break
	}

	p.InitDev(dev)

	// Encode and send it down the writer...
	if id == ID_PICK_ANY {
		id = atomic.AddUint32(&p.txID, 1)
	}

	i := rand.Intn(len(p.writers))

	p.writer_locks[i].Lock()
	defer p.writer_locks[i].Unlock()

	binary.LittleEndian.PutUint32(p.writer_headers[i], dev)
	binary.LittleEndian.PutUint32(p.writer_headers[i][4:], id)
	binary.LittleEndian.PutUint32(p.writer_headers[i][8:], length)

	n, err := p.writers[i].Write(p.writer_headers[i])
	atomic.AddUint64(&p.Metric_sent_packets, 1)
	atomic.AddUint64(&p.Metric_sent_bytes, uint64(n))

	n, err = p.writers[i].Write(header)
	atomic.AddUint64(&p.Metric_sent_bytes, uint64(n))

	cmd := header[0]
	p.metric_sent_packets_lock.Lock()
	p.metric_sent_packets_by_type[cmd]++
	p.metric_sent_packets_lock.Unlock()

	if err != nil {
		return 0, err
	}
	err = data(p.writers[i])
	if err == nil {
		atomic.AddUint64(&p.Metric_sent_bytes, uint64(length))
	}
	return id, err
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

	p.InitDev(dev)

	// Encode and send it down the writer...
	if id == ID_PICK_ANY {
		id = atomic.AddUint32(&p.txID, 1)
	}

	i := rand.Intn(len(p.writers))

	p.writer_locks[i].Lock()
	defer p.writer_locks[i].Unlock()

	binary.LittleEndian.PutUint32(p.writer_headers[i], dev)
	binary.LittleEndian.PutUint32(p.writer_headers[i][4:], id)
	binary.LittleEndian.PutUint32(p.writer_headers[i][8:], uint32(len(data)))

	n, err := p.writers[i].Write(p.writer_headers[i])

	if err != nil {
		return 0, err
	}
	atomic.AddUint64(&p.Metric_sent_packets, 1)
	atomic.AddUint64(&p.Metric_sent_bytes, uint64(n))

	n, err = p.writers[i].Write(data)
	if err == nil {
		atomic.AddUint64(&p.Metric_sent_bytes, uint64(n))

		cmd := data[0]
		p.metric_sent_packets_lock.Lock()
		p.metric_sent_packets_by_type[cmd]++
		p.metric_sent_packets_lock.Unlock()
	}
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
				}

				n, err := io.ReadFull(reader, header)
				if err != nil {
					errs <- err
					return
				}
				atomic.AddUint64(&p.Metric_recv_packets, 1)
				atomic.AddUint64(&p.Metric_recv_bytes, uint64(n))
				dev := binary.LittleEndian.Uint32(header)
				id := binary.LittleEndian.Uint32(header[4:])
				length := binary.LittleEndian.Uint32(header[8:])

				data := make([]byte, length)
				n, err = io.ReadFull(reader, data)

				if err != nil {
					errs <- err
					return
				}
				atomic.AddUint64(&p.Metric_recv_bytes, uint64(n))
				cmd := data[0]
				p.metric_recv_packets_lock.Lock()
				p.metric_recv_packets_by_type[cmd]++
				p.metric_recv_packets_lock.Unlock()

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
	p.InitDev(dev)

	if data == nil || len(data) < 1 {
		return errors.New("invalid data packet")
	}

	cmd := data[0]

	p.waiters_lock.Lock()
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

	p.waiters_lock.Unlock()

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
	mp.waiters_lock.Lock()
	w := mp.waiters[dev]
	wq, okk := w.byID[id]
	if !okk {
		wq = make(chan packetinfo, 8) // Some buffer here...
		w.byID[id] = wq
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
	w := mp.waiters[dev]
	wq, okk := w.byCmd[cmd]
	if !okk {
		wq = make(chan packetinfo, 8) // Some buffer here...
		w.byCmd[cmd] = wq
	}
	mp.waiters_lock.Unlock()

	select {
	case p := <-wq:
		return p.id, p.data, nil
	case <-mp.ctx.Done():
		return 0, nil, mp.ctx.Err()
	}
}
