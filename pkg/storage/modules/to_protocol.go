package modules

import (
	"github.com/loopholelabs/silo/pkg/storage/protocol"
)

type ToProtocol struct {
	size     uint64
	dev      uint32
	protocol protocol.Protocol
}

func NewToProtocol(size uint64, deviceID uint32, p protocol.Protocol) *ToProtocol {
	return &ToProtocol{
		size:     size,
		dev:      deviceID,
		protocol: p,
	}
}

func (i *ToProtocol) DirtyList(blocks []uint) error {
	b := protocol.EncodeDirtyList(blocks)
	_, err := i.protocol.SendPacket(i.dev, protocol.ID_PICK_ANY, b)
	return err
}

func (i *ToProtocol) ReadAt(buffer []byte, offset int64) (int, error) {
	b := protocol.EncodeReadAt(offset, int32(len(buffer)))
	id, err := i.protocol.SendPacket(i.dev, protocol.ID_PICK_ANY, b)
	if err != nil {
		return 0, err
	}
	// Wait for the response...
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return 0, err
	}

	// Decode the response and use it...
	rp, err := protocol.DecodeReadAtResponse(r)
	if err != nil {
		return 0, err
	}

	copy(buffer, rp.Data)

	return rp.Bytes, rp.Error
}

func (i *ToProtocol) WriteAt(buffer []byte, offset int64) (int, error) {
	b := protocol.EncodeWriteAt(offset, buffer)
	id, err := i.protocol.SendPacket(i.dev, protocol.ID_PICK_ANY, b)
	if err != nil {
		return 0, err
	}
	// Wait for the response...
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return 0, err
	}

	rp, err := protocol.DecodeWriteAtResponse(r)
	if err != nil {
		return 0, err
	}

	return rp.Bytes, rp.Error
}

func (i *ToProtocol) Flush() error {
	// TODO...
	return nil
}

func (i *ToProtocol) Size() uint64 {
	return i.size
}

// Handle any NeedAt commands, and send to an orderer...
func (i *ToProtocol) HandleNeedAt(cb func(offset int64, length int32)) error {
	for {
		_, data, err := i.protocol.WaitForCommand(i.dev, protocol.COMMAND_NEED_AT)
		if err != nil {
			return err
		}
		offset, length, err := protocol.DecodeNeedAt(data)
		if err != nil {
			return err
		}

		// We could spin up a goroutine here, but the assumption is that cb won't take long.
		cb(offset, length)
	}
}

// Handle any DontNeedAt commands, and send to an orderer...
func (i *ToProtocol) HandleDontNeedAt(cb func(offset int64, length int32)) error {
	for {
		_, data, err := i.protocol.WaitForCommand(i.dev, protocol.COMMAND_DONT_NEED_AT)
		if err != nil {
			return err
		}
		offset, length, err := protocol.DecodeDontNeedAt(data)
		if err != nil {
			return err
		}

		// We could spin up a goroutine here, but the assumption is that cb won't take long.
		cb(offset, length)
	}
}
