package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

type ToProtocol struct {
	storage.ProviderWithEvents
	size             uint64
	dev              uint32
	protocol         Protocol
	CompressedWrites bool
	alternateSources []packets.AlternateSource
}

func NewToProtocol(size uint64, deviceID uint32, p Protocol) *ToProtocol {
	return &ToProtocol{
		size:             size,
		dev:              deviceID,
		protocol:         p,
		CompressedWrites: false,
	}
}

// Support Silo Events
func (i *ToProtocol) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	if eventType == storage.EventType("sources") {
		i.alternateSources = eventData.([]packets.AlternateSource)
		// Send the list of alternate sources here...
		h := packets.EncodeAlternateSources(i.alternateSources)
		_, _ = i.protocol.SendPacket(i.dev, IDPickAny, h)
		// For now, we do not check the error. If there was a protocol / io error, we should see it on the next send
	}
	return nil
}

func (i *ToProtocol) SendEvent(e *packets.Event) error {
	b := packets.EncodeEvent(e)
	id, err := i.protocol.SendPacket(i.dev, IDPickAny, b)
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
	id, err := i.protocol.SendPacket(i.dev, IDPickAny, h)
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

func (i *ToProtocol) SendDevInfo(name string, blockSize uint32, schema string) error {
	di := &packets.DevInfo{
		Size:      i.size,
		BlockSize: blockSize,
		Name:      name,
		Schema:    schema,
	}
	b := packets.EncodeDevInfo(di)
	_, err := i.protocol.SendPacket(i.dev, IDPickAny, b)
	return err
}

func (i *ToProtocol) RemoveDev() error {
	f := packets.EncodeRemoveDev()
	_, err := i.protocol.SendPacket(i.dev, IDPickAny, f)
	return err
}

func (i *ToProtocol) DirtyList(blockSize int, blocks []uint) error {
	b := packets.EncodeDirtyList(blockSize, blocks)
	id, err := i.protocol.SendPacket(i.dev, IDPickAny, b)
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
	id, err := i.protocol.SendPacket(i.dev, IDPickAny, b)
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
				id, err = i.protocol.SendPacket(i.dev, IDPickAny, data)
				dontSendData = true
			}
			break
		}
	}

	if !dontSendData {
		if i.CompressedWrites {
			data := packets.EncodeWriteAtComp(offset, buffer)
			id, err = i.protocol.SendPacket(i.dev, IDPickAny, data)
		} else {
			data := packets.EncodeWriteAt(offset, buffer)
			id, err = i.protocol.SendPacket(i.dev, IDPickAny, data)
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
		return 0, packets.ErrInvalidPacket
	}
	if r[0] == packets.CommandWriteAtResponseErr {
		return 0, packets.ErrWriteError
	} else if r[0] == packets.CommandWriteAtResponse {
		if len(r) < 5 {
			return 0, packets.ErrInvalidPacket
		}
		return int(binary.LittleEndian.Uint32(r[1:])), nil
	}
	return 0, packets.ErrInvalidPacket
}

func (i *ToProtocol) WriteAtWithMap(buffer []byte, offset int64, idMap map[uint64]uint64) (int, error) {
	var id uint32
	var err error
	f := packets.EncodeWriteAtWithMap(offset, buffer, idMap)
	id, err = i.protocol.SendPacket(i.dev, IDPickAny, f)
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
		return 0, packets.ErrInvalidPacket
	}
	if r[0] == packets.CommandWriteAtResponseErr {
		return 0, packets.ErrWriteError
	} else if r[0] == packets.CommandWriteAtResponse {
		if len(r) < 5 {
			return 0, packets.ErrInvalidPacket
		}
		return int(binary.LittleEndian.Uint32(r[1:])), nil
	}
	return 0, packets.ErrInvalidPacket
}

func (i *ToProtocol) RemoveFromMap(ids []uint64) error {
	f := packets.EncodeRemoveFromMap(ids)
	_, err := i.protocol.SendPacket(i.dev, IDPickAny, f)
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

func (i *ToProtocol) CancelWrites(_ int64, _ int64) {
	// TODO: Implement
}

// Handle any NeedAt commands, and send to an orderer...
func (i *ToProtocol) HandleNeedAt(cb func(offset int64, length int32)) error {
	for {
		_, data, err := i.protocol.WaitForCommand(i.dev, packets.CommandNeedAt)
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
		_, data, err := i.protocol.WaitForCommand(i.dev, packets.CommandDontNeedAt)
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
