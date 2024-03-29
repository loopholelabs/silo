package protocol

import (
	"io"
	"time"
)

type TestProtocolLatency struct {
	proto       Protocol
	recvLatency time.Duration
}

func NewTestProtocolLatency(proto Protocol, recvLatency time.Duration) Protocol {
	p := &TestProtocolLatency{
		proto:       proto,
		recvLatency: recvLatency,
	}

	return p
}

func (p *TestProtocolLatency) SendPacketWriter(dev uint32, id uint32, length uint32, data func(w io.Writer) error) (uint32, error) {
	return p.proto.SendPacketWriter(dev, id, length, data)
}

func (p *TestProtocolLatency) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	return p.proto.SendPacket(dev, id, data)
}

func (p *TestProtocolLatency) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	time.Sleep(p.recvLatency)
	return p.proto.WaitForPacket(dev, id)
}

func (p *TestProtocolLatency) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	time.Sleep(p.recvLatency)
	return p.proto.WaitForCommand(dev, cmd)
}
