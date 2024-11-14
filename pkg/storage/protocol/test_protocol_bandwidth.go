package protocol

import (
	"sync"
	"time"
)

type packetInfo struct {
	ctime time.Time
	bytes uint64
}

type TestProtocolBandwidth struct {
	proto             Protocol
	recvBandwidth     uint64 // In bytes per second
	recentPacketsLock sync.Mutex
	recentPackets     []*packetInfo
}

func NewTestProtocolBandwidth(proto Protocol, recvBandwidth uint64) Protocol {
	p := &TestProtocolBandwidth{
		proto:         proto,
		recvBandwidth: recvBandwidth,
		recentPackets: make([]*packetInfo, 0),
	}

	return p
}

func (p *TestProtocolBandwidth) addRecentPacket(l uint64) {
	p.recentPacketsLock.Lock()
	defer p.recentPacketsLock.Unlock()

	// Inside lock
	p.waitForBandwidth()

	p.recentPackets = append(p.recentPackets, &packetInfo{
		ctime: time.Now(),
		bytes: l,
	})
}

func (p *TestProtocolBandwidth) checkBandwidth(since time.Duration) (uint64, time.Duration) {
	totalBytes := uint64(0)
	earliestTime := time.Now()
	for _, pi := range p.recentPackets {
		if time.Since(pi.ctime) < since {
			totalBytes += pi.bytes
			if pi.ctime.Before(earliestTime) {
				earliestTime = pi.ctime
			}
		}
	}
	return totalBytes, time.Since(earliestTime)
}

func (p *TestProtocolBandwidth) expireBandwidth(since time.Duration) {
	newRecentPackets := make([]*packetInfo, 0)
	for _, pi := range p.recentPackets {
		if time.Since(pi.ctime) < since {
			newRecentPackets = append(newRecentPackets, pi)
		}
	}
	p.recentPackets = newRecentPackets
}

func (p *TestProtocolBandwidth) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	return p.proto.SendPacket(dev, id, data)
}

func (p *TestProtocolBandwidth) waitForBandwidth() {
	for {
		bytes, duration := p.checkBandwidth(1 * time.Second)
		if duration.Nanoseconds() > 0 {
			// duration may be less than a second, in which case we should adjust bytes...
			sf := float64(time.Second.Nanoseconds()) / float64(duration.Nanoseconds())
			adjBytes := uint64(float64(bytes) * sf)
			if adjBytes < p.recvBandwidth {
				p.expireBandwidth(1 * time.Second)
				break
			}
		} else {
			break
		}
		// Wait a little to free some bandwidth...
		time.Sleep(5 * time.Millisecond)
	}
}

func (p *TestProtocolBandwidth) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	data, err := p.proto.WaitForPacket(dev, id)
	p.addRecentPacket(uint64(len(data) + 12))
	return data, err
}

func (p *TestProtocolBandwidth) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	id, data, err := p.proto.WaitForCommand(dev, cmd)
	p.addRecentPacket(uint64(len(data) + 12))
	return id, data, err
}
