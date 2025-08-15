package protocol

import "github.com/loopholelabs/silo/pkg/storage/protocol/packets"

// Mim lets us be a man in the middle in the protocol
type Mim struct {
	p                             Protocol
	PostSendPacket                func(dev uint32, id uint32, data []byte, urgency Urgency, pid uint32, err error)
	PostSendDeviceGroupInfo       func(dev uint32, id uint32, dgi *packets.DeviceGroupInfo, err error)
	PostSendAlternateSources      func(dev uint32, id uint32, as []packets.AlternateSource, err error)
	PostSendWriteAtData           func(dev uint32, id uint32, offset int64, data []byte, err error)
	PostSendWriteAtYouAlreadyHave func(dev uint32, id uint32, blocksize uint64, blocks []uint32, err error)
	PostSendWriteAtHash           func(dev uint32, id uint32, offset int64, length int64, hash []byte, loc packets.DataLocation, err error)
	PostWaitForPacket             func(dev uint32, id uint32, data []byte, err error)
	PostWaitForCommand            func(dev uint32, cmd byte, id uint32, data []byte, err error)
}

func NewMim(p Protocol) *Mim {
	return &Mim{
		p: p,
	}
}

func (m *Mim) SendPacket(dev uint32, id uint32, data []byte, urgency Urgency) (uint32, error) {
	pid, err := m.p.SendPacket(dev, id, data, urgency)
	if m.PostSendPacket != nil {
		m.PostSendPacket(dev, id, data, urgency, pid, err)
	}
	if len(data) > 0 {
		switch data[0] {
		case packets.CommandDeviceGroupInfo:
			if m.PostSendDeviceGroupInfo != nil {
				dgi, err := packets.DecodeDeviceGroupInfo(data)
				m.PostSendDeviceGroupInfo(dev, id, dgi, err)
			}
		case packets.CommandAlternateSources:
			if m.PostSendAlternateSources != nil {
				as, err := packets.DecodeAlternateSources(data)
				m.PostSendAlternateSources(dev, id, as, err)
			}
		case packets.CommandWriteAt:
			switch data[1] {
			case packets.WriteAtData:
				if m.PostSendWriteAtData != nil {
					off, d, err := packets.DecodeWriteAt(data)
					m.PostSendWriteAtData(dev, id, off, d, err)
				}
			case packets.WriteAtHash:
				if m.PostSendWriteAtHash != nil {
					off, length, hash, loc, err := packets.DecodeWriteAtHash(data)
					m.PostSendWriteAtHash(dev, id, off, length, hash, loc, err)
				}
			case packets.WriteAtYouAlreadyHave:
				if m.PostSendWriteAtYouAlreadyHave != nil {
					blocksize, blocks, err := packets.DecodeYouAlreadyHave(data)
					m.PostSendWriteAtYouAlreadyHave(dev, id, blocksize, blocks, err)
				}

			case packets.WriteAtCompRLE:
				if m.PostSendWriteAtData != nil {
					off, d, err := packets.DecodeWriteAtCompRLE(data)
					m.PostSendWriteAtData(dev, id, off, d, err)
				}
			case packets.WriteAtCompGzip:
				if m.PostSendWriteAtData != nil {
					off, d, err := packets.DecodeWriteAtCompGzip(data)
					m.PostSendWriteAtData(dev, id, off, d, err)
				}
			case packets.WriteAtCompZeroes:
				if m.PostSendWriteAtData != nil {
					off, d, err := packets.DecodeWriteAtCompZeroes(data)
					m.PostSendWriteAtData(dev, id, off, d, err)
				}
			}
		}
	}
	return pid, err
}

func (m *Mim) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	data, err := m.p.WaitForPacket(dev, id)
	if m.PostWaitForPacket != nil {
		m.PostWaitForPacket(dev, id, data, err)
	}
	return data, err
}

func (m *Mim) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	id, data, err := m.p.WaitForCommand(dev, cmd)
	if m.PostWaitForCommand != nil {
		m.PostWaitForCommand(dev, cmd, id, data, err)
	}
	return id, data, err
}
