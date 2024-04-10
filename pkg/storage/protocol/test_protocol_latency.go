package protocol

import (
	"io"
	"sync/atomic"
	"time"
)

/**
 * FIXME: This is lame, doesn't accurately model latency.
 *
 */

type TestProtocolLatency struct {
	proto       Protocol
	recvLatency time.Duration
	isFirst     atomic.Bool
}

func NewTestProtocolLatency(proto Protocol, recvLatency time.Duration) Protocol {
	p := &TestProtocolLatency{
		proto:       proto,
		recvLatency: recvLatency,
	}

	p.isFirst.Store(true)
	return p
}

func (p *TestProtocolLatency) SendPacketWriter(dev uint32, id uint32, length uint32, data func(w io.Writer) error) (uint32, error) {
	return p.proto.SendPacketWriter(dev, id, length, data)
}

func (p *TestProtocolLatency) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	return p.proto.SendPacket(dev, id, data)
}

func (p *TestProtocolLatency) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	if p.isFirst.Load() {
		time.Sleep(p.recvLatency)
		p.isFirst.Store(false)
	}
	return p.proto.WaitForPacket(dev, id)
}

func (p *TestProtocolLatency) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	if p.isFirst.Load() {
		time.Sleep(p.recvLatency)
		p.isFirst.Store(false)
	}
	return p.proto.WaitForCommand(dev, cmd)
}
