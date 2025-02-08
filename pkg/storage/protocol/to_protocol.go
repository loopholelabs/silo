package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

type ToProtocol struct {
	storage.ProviderWithEvents
	baseImage                      storage.Provider
	baseBlocks                     map[uint]uint
	baseImageLock                  sync.Mutex
	size                           uint64
	dev                            uint32
	protocol                       Protocol
	compressedWrites               atomic.Bool
	alternateSources               []packets.AlternateSource
	metricSentEvents               uint64
	metricSentAltSources           uint64
	metricSentHashes               uint64
	metricSentDevInfo              uint64
	metricSentRemoveDev            uint64
	metricSentDirtyList            uint64
	metricSentReadAt               uint64
	metricSentWriteAtHash          uint64
	metricSentWriteAtHashBytes     uint64
	metricSentWriteAtComp          uint64
	metricSentWriteAtCompBytes     uint64
	metricSentWriteAtCompDataBytes uint64
	metricSentWriteAt              uint64
	metricSentWriteAtBytes         uint64
	metricSentWriteAtWithMap       uint64
	metricSentRemoveFromMap        uint64
	metricRecvNeedAt               uint64
	metricRecvDontNeedAt           uint64
}

func NewToProtocol(size uint64, deviceID uint32, p Protocol) *ToProtocol {
	return &ToProtocol{
		size:     size,
		dev:      deviceID,
		protocol: p,
	}
}

type ToProtocolMetrics struct {
	SentEvents               uint64
	SentAltSources           uint64
	SentHashes               uint64
	SentDevInfo              uint64
	SentRemoveDev            uint64
	SentDirtyList            uint64
	SentReadAt               uint64
	SentWriteAtHash          uint64
	SentWriteAtHashBytes     uint64
	SentWriteAtComp          uint64
	SentWriteAtCompBytes     uint64
	SentWriteAtCompDataBytes uint64
	SentWriteAt              uint64
	SentWriteAtBytes         uint64
	SentWriteAtWithMap       uint64
	SentRemoveFromMap        uint64
	RecvNeedAt               uint64
	RecvDontNeedAt           uint64
}

func (i *ToProtocol) GetMetrics() *ToProtocolMetrics {
	return &ToProtocolMetrics{
		SentEvents:               atomic.LoadUint64(&i.metricSentEvents),
		SentAltSources:           atomic.LoadUint64(&i.metricSentAltSources),
		SentHashes:               atomic.LoadUint64(&i.metricSentHashes),
		SentDevInfo:              atomic.LoadUint64(&i.metricSentDevInfo),
		SentRemoveDev:            atomic.LoadUint64(&i.metricSentRemoveDev),
		SentDirtyList:            atomic.LoadUint64(&i.metricSentDirtyList),
		SentReadAt:               atomic.LoadUint64(&i.metricSentReadAt),
		SentWriteAtHash:          atomic.LoadUint64(&i.metricSentWriteAtHash),
		SentWriteAtHashBytes:     atomic.LoadUint64(&i.metricSentWriteAtHashBytes),
		SentWriteAtComp:          atomic.LoadUint64(&i.metricSentWriteAtComp),
		SentWriteAtCompBytes:     atomic.LoadUint64(&i.metricSentWriteAtCompBytes),
		SentWriteAtCompDataBytes: atomic.LoadUint64(&i.metricSentWriteAtCompDataBytes),
		SentWriteAt:              atomic.LoadUint64(&i.metricSentWriteAt),
		SentWriteAtBytes:         atomic.LoadUint64(&i.metricSentWriteAtBytes),
		SentWriteAtWithMap:       atomic.LoadUint64(&i.metricSentWriteAtWithMap),
		SentRemoveFromMap:        atomic.LoadUint64(&i.metricSentRemoveFromMap),
		RecvNeedAt:               atomic.LoadUint64(&i.metricRecvNeedAt),
		RecvDontNeedAt:           atomic.LoadUint64(&i.metricRecvDontNeedAt),
	}
}

// Support Silo Events
func (i *ToProtocol) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	if eventType == storage.EventTypeSources {
		i.alternateSources = eventData.([]packets.AlternateSource)
		// Send the list of alternate sources here...
		i.SendAltSources(i.alternateSources)
		// For now, we do not check the error. If there was a protocol / io error, we should see it on the next send
	} else if eventType == storage.EventTypeBaseSet {
		// We have been told a base image that we can use when migrating CoW or similar.
		i.baseImageLock.Lock()
		i.baseImage = eventData.(storage.Provider)
		i.baseImageLock.Unlock()
	} else if eventType == storage.EventTypeCowSetBlocks {
		// We have been told which blocks are in the CoW overlay
		i.baseImageLock.Lock()
		i.baseBlocks = eventData.(map[uint]uint)
		i.baseImageLock.Unlock()
	}
	return nil
}

func (i *ToProtocol) SetCompression(compressed bool) {
	i.compressedWrites.Store(compressed)
}

func (i *ToProtocol) SendAltSources(s []packets.AlternateSource) error {
	h := packets.EncodeAlternateSources(s)
	_, err := i.protocol.SendPacket(i.dev, IDPickAny, h, UrgencyUrgent)
	if err == nil {
		atomic.AddUint64(&i.metricSentAltSources, 1)
	}
	return err
}

func (i *ToProtocol) SendEvent(e *packets.Event) error {
	b := packets.EncodeEvent(e)
	id, err := i.protocol.SendPacket(i.dev, IDPickAny, b, UrgencyUrgent)
	if err != nil {
		return err
	}

	atomic.AddUint64(&i.metricSentEvents, 1)

	// Wait for acknowledgement
	r, err := i.protocol.WaitForPacket(i.dev, id)
	if err != nil {
		return err
	}

	return packets.DecodeEventResponse(r)
}

func (i *ToProtocol) SendHashes(hashes map[uint][sha256.Size]byte) error {
	h := packets.EncodeHashes(hashes)
	id, err := i.protocol.SendPacket(i.dev, IDPickAny, h, UrgencyUrgent)
	if err != nil {
		return err
	}

	atomic.AddUint64(&i.metricSentHashes, 1)

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
	_, err := i.protocol.SendPacket(i.dev, IDPickAny, b, UrgencyUrgent)
	if err != nil {
		return err
	}

	atomic.AddUint64(&i.metricSentDevInfo, 1)
	return err
}

func (i *ToProtocol) RemoveDev() error {
	f := packets.EncodeRemoveDev()
	_, err := i.protocol.SendPacket(i.dev, IDPickAny, f, UrgencyUrgent)
	if err != nil {
		return err
	}

	atomic.AddUint64(&i.metricSentRemoveDev, 1)

	return nil
}

func (i *ToProtocol) DirtyList(blockSize int, blocks []uint) error {
	b := packets.EncodeDirtyList(blockSize, blocks)
	id, err := i.protocol.SendPacket(i.dev, IDPickAny, b, UrgencyUrgent)
	if err != nil {
		return err
	}

	atomic.AddUint64(&i.metricSentDirtyList, 1)

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
	id, err := i.protocol.SendPacket(i.dev, IDPickAny, b, UrgencyNormal)
	if err != nil {
		return 0, err
	}

	atomic.AddUint64(&i.metricSentReadAt, 1)

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

	dontSendData := false

	// Check if the data is in a base image. If so, send a WriteAtHash command instead of the data.
	var baseProv storage.Provider
	var baseBlocks map[uint]uint
	i.baseImageLock.Lock()
	baseProv = i.baseImage
	baseBlocks = i.baseBlocks
	i.baseImageLock.Unlock()

	if baseProv != nil {
		hash := sha256.Sum256(buffer)
		baseBuffer := make([]byte, len(buffer))
		n, err := baseProv.ReadAt(baseBuffer, offset)
		if err == nil && n == len(baseBuffer) {
			baseHash := sha256.Sum256(baseBuffer)
			if bytes.Equal(hash[:], baseHash[:]) {
				// The data is exactly the same as our "base" image. Send it as WriteAtHash commands.
				data := packets.EncodeWriteAtHash(offset, int64(len(buffer)), hash[:], packets.DataLocationBaseImage)
				id, err = i.protocol.SendPacket(i.dev, IDPickAny, data, UrgencyNormal)
				if err == nil {
					atomic.AddUint64(&i.metricSentWriteAtHash, 1)
					atomic.AddUint64(&i.metricSentWriteAtHashBytes, uint64(len(buffer)))
				}
				dontSendData = true
			}
		}
	} else if baseBlocks != nil {
		if baseBlocks[uint(offset)] == uint(len(buffer)) {
			// It's part of the overlay. Send it as usual.
		} else {
			// The data is exactly the same as our "base" image. Send it as WriteAtHash commands.
			hash := make([]byte, sha256.Size) // Empty for now
			data := packets.EncodeWriteAtHash(offset, int64(len(buffer)), hash, packets.DataLocationBaseImage)
			id, err = i.protocol.SendPacket(i.dev, IDPickAny, data, UrgencyNormal)
			if err == nil {
				atomic.AddUint64(&i.metricSentWriteAtHash, 1)
				atomic.AddUint64(&i.metricSentWriteAtHashBytes, uint64(len(buffer)))
			}
			dontSendData = true
		}
	}

	// If it's in the alternateSources list, we only need to send a WriteAtHash command.
	// For now, we only match exact block ranges here.
	if !dontSendData {
		for _, as := range i.alternateSources {
			if as.Offset == offset && as.Length == int64(len(buffer)) {
				// Only allow this if the hash is still correct/current for the data.
				hash := sha256.Sum256(buffer)
				if bytes.Equal(hash[:], as.Hash[:]) {
					data := packets.EncodeWriteAtHash(as.Offset, as.Length, as.Hash[:], packets.DataLocationS3)
					id, err = i.protocol.SendPacket(i.dev, IDPickAny, data, UrgencyNormal)
					if err == nil {
						atomic.AddUint64(&i.metricSentWriteAtHash, 1)
						atomic.AddUint64(&i.metricSentWriteAtHashBytes, uint64(as.Length))
					}
					dontSendData = true
				}
				break
			}
		}
	}

	if !dontSendData {
		if i.compressedWrites.Load() {
			data := packets.EncodeWriteAtComp(offset, buffer)
			id, err = i.protocol.SendPacket(i.dev, IDPickAny, data, UrgencyNormal)
			if err == nil {
				atomic.AddUint64(&i.metricSentWriteAtComp, 1)
				atomic.AddUint64(&i.metricSentWriteAtCompBytes, uint64(len(buffer)))
				atomic.AddUint64(&i.metricSentWriteAtCompDataBytes, uint64(len(data)))
			}
		} else {
			data := packets.EncodeWriteAt(offset, buffer)
			id, err = i.protocol.SendPacket(i.dev, IDPickAny, data, UrgencyNormal)
			if err == nil {
				atomic.AddUint64(&i.metricSentWriteAt, 1)
				atomic.AddUint64(&i.metricSentWriteAtBytes, uint64(len(buffer)))
			}
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
	id, err = i.protocol.SendPacket(i.dev, IDPickAny, f, UrgencyNormal)
	if err != nil {
		return 0, err
	}

	atomic.AddUint64(&i.metricSentWriteAtWithMap, 1)

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
	_, err := i.protocol.SendPacket(i.dev, IDPickAny, f, UrgencyUrgent)
	if err == nil {
		atomic.AddUint64(&i.metricSentRemoveFromMap, 1)
	}
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

		atomic.AddUint64(&i.metricRecvNeedAt, 1)

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

		atomic.AddUint64(&i.metricRecvDontNeedAt, 1)

		// We could spin up a goroutine here, but the assumption is that cb won't take long.
		cb(offset, length)
	}
}
