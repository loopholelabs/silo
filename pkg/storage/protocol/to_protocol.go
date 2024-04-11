package protocol

import (
	"encoding/binary"
	"errors"
)

var ErrInvalidPacket = errors.New("invalid packet")
var ErrRemoteWriteError = errors.New("remote write error")

type ToProtocol struct {
	size             uint64
	dev              uint32
	protocol         Protocol
	CompressedWrites bool
}

func NewToProtocol(size uint64, deviceID uint32, p Protocol) *ToProtocol {
	return &ToProtocol{
		size:             size,
		dev:              deviceID,
		protocol:         p,
		CompressedWrites: false,
	}
}

func (i *ToProtocol) SendEvent(e *Event) error {
	b := EncodeEvent(e)
	id, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	if err != nil {
		return err
	}

	// Wait for acknowledgement
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return err
	}

	return DecodeEventResponse(r)
}

func (i *ToProtocol) SendDevInfo(name string, block_size uint32) error {
	di := &DevInfo{
		Size:      i.size,
		BlockSize: block_size,
		Name:      name,
	}
	b := EncodeDevInfo(di)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}

func (i *ToProtocol) DirtyList(blocks []uint) error {
	b := EncodeDirtyList(blocks)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}

func (i *ToProtocol) ReadAt(buffer []byte, offset int64) (int, error) {
	b := EncodeReadAt(offset, int32(len(buffer)))
	id, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	if err != nil {
		return 0, err
	}
	// Wait for the response...
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return 0, err
	}

	// Decode the response and use it...
	rp, err := DecodeReadAtResponse(r)
	if err != nil {
		return 0, err
	}

	copy(buffer, rp.Data)

	return rp.Bytes, rp.Error
}

func (i *ToProtocol) WriteAt(buffer []byte, offset int64) (int, error) {
	var id uint32
	var err error
	if i.CompressedWrites {
		data := EncodeWriteAtComp(offset, buffer)
		id, err = i.protocol.SendPacket(i.dev, ID_PICK_ANY, data)
	} else {
		l, f := EncodeWriterWriteAt(offset, buffer)
		id, err = i.protocol.SendPacketWriter(i.dev, ID_PICK_ANY, l, f)
	}
	if err != nil {
		return 0, err
	}
	// Wait for the response...
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return 0, err
	}

	// Decode the response...
	if r == nil || len(r) < 1 {
		return 0, ErrInvalidPacket
	}
	if r[0] == COMMAND_WRITE_AT_RESPONSE_ERR {
		return 0, ErrRemoteWriteError
	} else if r[0] == COMMAND_WRITE_AT_RESPONSE {
		if len(r) < 5 {
			return 0, ErrInvalidPacket
		}
		return int(binary.LittleEndian.Uint32(r[1:])), nil
	}
	return 0, ErrInvalidPacket
}

func (i *ToProtocol) Flush() error {
	// TODO...
	return nil
}

func (i *ToProtocol) Size() uint64 {
	return i.size
}

func (i *ToProtocol) Close() error {
	return nil
}

// Handle any NeedAt commands, and send to an orderer...
func (i *ToProtocol) HandleNeedAt(cb func(offset int64, length int32)) error {
	for {
		_, data, err := i.protocol.WaitForCommand(i.dev, COMMAND_NEED_AT)
		if err != nil {
			return err
		}
		offset, length, err := DecodeNeedAt(data)
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
		_, data, err := i.protocol.WaitForCommand(i.dev, COMMAND_DONT_NEED_AT)
		if err != nil {
			return err
		}
		offset, length, err := DecodeDontNeedAt(data)
		if err != nil {
			return err
		}

		// We could spin up a goroutine here, but the assumption is that cb won't take long.
		cb(offset, length)
	}
}
