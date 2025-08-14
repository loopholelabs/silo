package protocol

// Mim lets us be a man in the middle in the protocol
type Mim struct {
	p                  Protocol
	PostSendPacket     func(dev uint32, id uint32, data []byte, urgency Urgency, pid uint32, err error)
	PostWaitForPacket  func(dev uint32, id uint32, data []byte, err error)
	PostWaitForCommand func(dev uint32, cmd byte, id uint32, data []byte, err error)
}

func NewMim(p Protocol) Protocol {
	return &Mim{
		p: p,
	}
}

func (m *Mim) SendPacket(dev uint32, id uint32, data []byte, urgency Urgency) (uint32, error) {
	pid, err := m.p.SendPacket(dev, id, data, urgency)
	if m.PostSendPacket != nil {
		m.PostSendPacket(dev, id, data, urgency, pid, err)
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
