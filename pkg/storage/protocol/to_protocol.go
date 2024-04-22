package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
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

func (i *ToProtocol) SendEvent(e *packets.Event) error {
	b := packets.EncodeEvent(e)
	id, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	if err != nil {
		return err
	}

	// Wait for acknowledgement
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return err
	}

	return packets.DecodeEventResponse(r)
}

func (i *ToProtocol) SendHashes(hashes map[uint][sha256.Size]byte) error {
	h := packets.EncodeHashes(hashes)
	id, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, h)
	if err != nil {
		return err
	}
	// Wait for an ack
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return err
	}

	return packets.DecodeHashesResponse(r)
}

func (i *ToProtocol) SendDevInfo(name string, block_size uint32) error {
	di := &packets.DevInfo{
		Size:      i.size,
		BlockSize: block_size,
		Name:      name,
	}
	b := packets.EncodeDevInfo(di)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}

func (i *ToProtocol) DirtyList(blocks []uint) error {
	b := packets.EncodeDirtyList(blocks)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}

func (i *ToProtocol) ReadAt(buffer []byte, offset int64) (int, error) {
	b := packets.EncodeReadAt(offset, int32(len(buffer)))
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
	rp, err := packets.DecodeReadAtResponse(r)
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
		data := packets.EncodeWriteAtComp(offset, buffer)
		id, err = i.protocol.SendPacket(i.dev, ID_PICK_ANY, data)
	} else {
		l, f := packets.EncodeWriterWriteAt(offset, buffer)
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
	if r[0] == packets.COMMAND_WRITE_AT_RESPONSE_ERR {
		return 0, ErrRemoteWriteError
	} else if r[0] == packets.COMMAND_WRITE_AT_RESPONSE {
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
		_, data, err := i.protocol.WaitForCommand(i.dev, packets.COMMAND_NEED_AT)
		if err != nil {
			return err
		}
		offset, length, err := packets.DecodeNeedAt(data)
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
		_, data, err := i.protocol.WaitForCommand(i.dev, packets.COMMAND_DONT_NEED_AT)
		if err != nil {
			return err
		}
		offset, length, err := packets.DecodeDontNeedAt(data)
		if err != nil {
			return err
		}

		// We could spin up a goroutine here, but the assumption is that cb won't take long.
		cb(offset, length)
	}
}
