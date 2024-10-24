package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

var ErrInvalidPacket = errors.New("invalid packet")
var ErrRemoteWriteError = errors.New("remote write error")

type ToProtocol struct {
	storage.StorageProviderWithEvents
	size              uint64
	dev               uint32
	protocol          Protocol
	Compressed_writes bool
	alternateSources  []packets.AlternateSource
}

func NewToProtocol(size uint64, deviceID uint32, p Protocol) *ToProtocol {
	return &ToProtocol{
		size:              size,
		dev:               deviceID,
		protocol:          p,
		Compressed_writes: false,
	}
}

// Support Silo Events
func (i *ToProtocol) SendSiloEvent(event_type storage.EventType, event_data storage.EventData) []storage.EventReturnData {
	if event_type == storage.EventType("sources") {
		i.alternateSources = event_data.([]packets.AlternateSource)
		// Send the list of alternate sources here...
		h := packets.EncodeAlternateSources(i.alternateSources)
		_, _ = i.protocol.SendPacket(i.dev, ID_PICK_ANY, h)
		// For now, we do not check the error. If there was a protocol / io error, we should see it on the next send
	}
	return nil
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

func (i *ToProtocol) SendDevInfo(name string, block_size uint32, schema string) error {
	di := &packets.DevInfo{
		Size:       i.size,
		Block_size: block_size,
		Name:       name,
		Schema:     schema,
	}
	b := packets.EncodeDevInfo(di)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}

func (i *ToProtocol) RemoveDev() error {
	f := packets.EncodeRemoveDev()
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, f)
	return err
}

func (i *ToProtocol) DirtyList(block_size int, blocks []uint) error {
	b := packets.EncodeDirtyList(block_size, blocks)
	id, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	if err != nil {
		return err
	}

	// Wait for the response...
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return err
	}

	// Decode the response and use it...
	err = packets.DecodeDirtyListResponse(r)
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

	// If it's in the alternateSources list, we only need to send a WriteAtHash command.
	// For now, we only match exact block ranges here.
	dontSendData := false
	for _, as := range i.alternateSources {
		if as.Offset == offset && as.Length == int64(len(buffer)) {
			// Only allow this if the hash is still correct/current for the data.
			hash := sha256.Sum256(buffer)
			if bytes.Equal(hash[:], as.Hash[:]) {
				data := packets.EncodeWriteAtHash(as.Offset, as.Length, as.Hash[:])
				id, err = i.protocol.SendPacket(i.dev, ID_PICK_ANY, data)
				dontSendData = true
			}
			break
		}
	}

	if !dontSendData {
		if i.Compressed_writes {
			data := packets.EncodeWriteAtComp(offset, buffer)
			id, err = i.protocol.SendPacket(i.dev, ID_PICK_ANY, data)
		} else {
			l, f := packets.EncodeWriterWriteAt(offset, buffer)
			id, err = i.protocol.SendPacketWriter(i.dev, ID_PICK_ANY, l, f)
		}
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

func (i *ToProtocol) WriteAtWithMap(buffer []byte, offset int64, id_map map[uint64]uint64) (int, error) {
	var id uint32
	var err error
	f := packets.EncodeWriteAtWithMap(offset, buffer, id_map)
	id, err = i.protocol.SendPacket(i.dev, ID_PICK_ANY, f)
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

func (i *ToProtocol) RemoveFromMap(ids []uint64) error {
	f := packets.EncodeRemoveFromMap(ids)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, f)
	return err
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

func (i *ToProtocol) CancelWrites(offset int64, length int64) {
	// TODO: Implement
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
