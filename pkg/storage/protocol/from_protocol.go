package protocol

import (
	"context"
	"crypto/sha256"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type FromProtocol struct {
	dev             uint32
	prov            storage.Provider
	providerFactory func(*packets.DevInfo) storage.Provider
	protocol        Protocol
	init            chan bool
	ctx             context.Context

	presentLock      sync.Mutex
	present          *util.Bitfield
	presentBlockSize int

	alternateSourcesLock sync.Mutex
	alternateSources     []packets.AlternateSource

	metricRecvEvents         uint64
	metricRecvHashes         uint64
	metricRecvDevInfo        uint64
	metricRecvAltSources     uint64
	metricRecvReadAt         uint64
	metricRecvWriteAtHash    uint64
	metricRecvWriteAtComp    uint64
	metricRecvWriteAt        uint64
	metricRecvWriteAtWithMap uint64
	metricRecvRemoveFromMap  uint64
	metricRecvRemoveDev      uint64
	metricRecvDirtyList      uint64
	metricSentNeedAt         uint64
	metricSentDontNeedAt     uint64
}

type FromProtocolMetrics struct {
	RecvEvents         uint64
	RecvHashes         uint64
	RecvDevInfo        uint64
	RecvAltSources     uint64
	RecvReadAt         uint64
	RecvWriteAtHash    uint64
	RecvWriteAtComp    uint64
	RecvWriteAt        uint64
	RecvWriteAtWithMap uint64
	RecvRemoveFromMap  uint64
	RecvRemoveDev      uint64
	RecvDirtyList      uint64
	SentNeedAt         uint64
	SentDontNeedAt     uint64
}

func NewFromProtocol(ctx context.Context, dev uint32, provFactory func(*packets.DevInfo) storage.Provider, protocol Protocol) *FromProtocol {
	fp := &FromProtocol{
		dev:              dev,
		providerFactory:  provFactory,
		protocol:         protocol,
		presentBlockSize: 1024,
		init:             make(chan bool, 1),
		ctx:              ctx,
	}
	return fp
}

func (fp *FromProtocol) GetMetrics() *FromProtocolMetrics {
	return &FromProtocolMetrics{
		RecvEvents:         atomic.LoadUint64(&fp.metricRecvEvents),
		RecvHashes:         atomic.LoadUint64(&fp.metricRecvHashes),
		RecvDevInfo:        atomic.LoadUint64(&fp.metricRecvDevInfo),
		RecvAltSources:     atomic.LoadUint64(&fp.metricRecvAltSources),
		RecvReadAt:         atomic.LoadUint64(&fp.metricRecvReadAt),
		RecvWriteAtHash:    atomic.LoadUint64(&fp.metricRecvWriteAtHash),
		RecvWriteAtComp:    atomic.LoadUint64(&fp.metricRecvWriteAtComp),
		RecvWriteAt:        atomic.LoadUint64(&fp.metricRecvWriteAt),
		RecvWriteAtWithMap: atomic.LoadUint64(&fp.metricRecvWriteAtWithMap),
		RecvRemoveFromMap:  atomic.LoadUint64(&fp.metricRecvRemoveFromMap),
		RecvRemoveDev:      atomic.LoadUint64(&fp.metricRecvRemoveDev),
		RecvDirtyList:      atomic.LoadUint64(&fp.metricRecvDirtyList),
		SentNeedAt:         atomic.LoadUint64(&fp.metricSentNeedAt),
		SentDontNeedAt:     atomic.LoadUint64(&fp.metricSentDontNeedAt),
	}
}

func (fp *FromProtocol) GetAlternateSources() []packets.AlternateSource {
	fp.alternateSourcesLock.Lock()
	defer fp.alternateSourcesLock.Unlock()

	// If we didn't get a WriteAt, then return the alternateSource for a block, and mark it as present.
	ret := make([]packets.AlternateSource, 0)
	for _, as := range fp.alternateSources {
		if !fp.isDataPresent(int(as.Offset), int(as.Length)) {
			ret = append(ret, as)
			// Mark the range as present if we return it here.
			fp.markRangePresent(int(as.Offset), int(as.Length))
		}
	}
	return ret
}

func (fp *FromProtocol) GetDataPresent() int {
	fp.presentLock.Lock()
	defer fp.presentLock.Unlock()
	blocks := fp.present.Count(0, fp.present.Length())
	size := blocks * fp.presentBlockSize
	if size > int(fp.prov.Size()) {
		size = int(fp.prov.Size())
	}
	return size
}

func (fp *FromProtocol) isDataPresent(offset int, length int) bool {
	// Translate to full blocks...
	end := uint64(offset + length)
	if end > fp.prov.Size() {
		end = fp.prov.Size()
	}

	bStart := uint(offset / fp.presentBlockSize)
	bEnd := uint((end-1)/uint64(fp.presentBlockSize)) + 1

	// Check they're all there
	fp.presentLock.Lock()
	defer fp.presentLock.Unlock()
	return fp.present.BitsSet(bStart, bEnd)
}

func (fp *FromProtocol) markRangePresent(offset int, length int) {
	// Translate to full blocks...
	end := uint64(offset + length)
	if end > fp.prov.Size() {
		end = fp.prov.Size()
	}

	bStart := uint(offset / fp.presentBlockSize)
	bEnd := uint((end-1)/uint64(fp.presentBlockSize)) + 1

	// First block is incomplete. Ignore it.
	if offset > (int(bStart) * fp.presentBlockSize) {
		bStart++
	}

	// Last block is incomplete AND is not the last block. Ignore it.
	if offset+length < int(bEnd*uint(fp.presentBlockSize)) && offset+length < int(fp.prov.Size()) {
		bEnd--
	}

	// Mark the blocks
	fp.presentLock.Lock()
	fp.present.SetBits(bStart, bEnd)
	fp.presentLock.Unlock()
}

func (fp *FromProtocol) markRangeMissing(offset int, length int) {
	// Translate to full blocks...
	end := uint64(offset + length)
	if end > fp.prov.Size() {
		end = fp.prov.Size()
	}

	bStart := uint(offset / fp.presentBlockSize)
	bEnd := uint((end-1)/uint64(fp.presentBlockSize)) + 1

	// First block is incomplete. Ignore it.
	if offset > (int(bStart) * fp.presentBlockSize) {
		bStart++
	}

	// Last block is incomplete AND is not the last block. Ignore it.
	if offset+length < int(bEnd*uint(fp.presentBlockSize)) && offset+length < int(fp.prov.Size()) {
		bEnd--
	}

	// Mark the blocks
	fp.presentLock.Lock()
	fp.present.ClearBits(bStart, bEnd)
	fp.presentLock.Unlock()
}

func (fp *FromProtocol) waitInitOrCancel() error {
	// Wait until init, or until context cancelled.
	select {
	case <-fp.init:
		fp.init <- true
		return nil
	case <-fp.ctx.Done():
		return fp.ctx.Err()
	}
}

// Handle any Events
func (fp *FromProtocol) HandleEvent(cb func(*packets.Event)) error {
	err := fp.waitInitOrCancel()
	if err != nil {
		return err
	}

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandEvent)
		if err != nil {
			return err
		}
		ev, err := packets.DecodeEvent(data)
		if err != nil {
			return err
		}

		atomic.AddUint64(&fp.metricRecvEvents, 1)

		if ev.Type == packets.EventCompleted {
			// Deal with the sync here, and WAIT if needed.
			storage.SendSiloEvent(fp.prov, "sync.start", storage.SyncStartConfig{
				AlternateSources: fp.GetAlternateSources(),
				Destination:      fp.prov,
			})
		}

		// Relay the event, wait, and then respond.
		cb(ev)

		_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeEventResponse(), UrgencyUrgent)
		if err != nil {
			return err
		}
	}
}

// Handle hashes
func (fp *FromProtocol) HandleHashes(cb func(map[uint][sha256.Size]byte)) error {
	err := fp.waitInitOrCancel()
	if err != nil {
		return err
	}

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandHashes)
		if err != nil {
			return err
		}
		hashes, err := packets.DecodeHashes(data)
		if err != nil {
			return err
		}

		atomic.AddUint64(&fp.metricRecvHashes, 1)

		// Relay the hashes, wait and then respond
		cb(hashes)

		_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeHashesResponse(), UrgencyUrgent)
		if err != nil {
			return err
		}
	}
}

// Handle a DevInfo, and create the storage
func (fp *FromProtocol) HandleDevInfo() error {
	_, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandDevInfo)
	if err != nil {
		return err
	}
	di, err := packets.DecodeDevInfo(data)
	if err != nil {
		return err
	}

	atomic.AddUint64(&fp.metricRecvDevInfo, 1)

	// Create storage
	fp.prov = fp.providerFactory(di)
	numBlocks := (int(fp.prov.Size()) + fp.presentBlockSize - 1) / fp.presentBlockSize
	fp.present = util.NewBitfield(numBlocks)

	fp.init <- true // Confirm things have been initialized for this device.

	// Internal - store alternateSources here...
	go func() {
		for {
			_, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandAlternateSources)
			if err != nil {
				return
			}
			altSources, err := packets.DecodeAlternateSources(data)
			if err != nil {
				return
			}

			atomic.AddUint64(&fp.metricRecvAltSources, 1)

			// For now just set it. It only gets sent ONCE at the start of a migration at the moment.
			fp.alternateSourcesLock.Lock()
			fp.alternateSources = altSources
			fp.alternateSourcesLock.Unlock()
		}
	}()

	return nil
}

// Handle any ReadAt commands, and send to provider
func (fp *FromProtocol) HandleReadAt() error {
	err := fp.waitInitOrCancel()
	if err != nil {
		return err
	}

	var errLock sync.Mutex
	var errValue error

	for {
		// If there was an error in one of the goroutines, return it.
		errLock.Lock()
		if errValue != nil {
			errLock.Unlock()
			return errValue
		}
		errLock.Unlock()

		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandReadAt)
		if err != nil {
			return err
		}
		offset, length, err := packets.DecodeReadAt(data)
		if err != nil {
			return err
		}

		atomic.AddUint64(&fp.metricRecvReadAt, 1)

		// Handle them in goroutines
		go func(goffset int64, glength int32, gid uint32) {
			buff := make([]byte, glength)
			n, err := fp.prov.ReadAt(buff, goffset)
			rar := &packets.ReadAtResponse{
				Bytes: n,
				Error: err,
				Data:  buff,
			}
			_, err = fp.protocol.SendPacket(fp.dev, gid, packets.EncodeReadAtResponse(rar), UrgencyNormal)
			if err != nil {
				errLock.Lock()
				errValue = err
				errLock.Unlock()
			}
		}(offset, length, id)
	}
}

// Handle any WriteAt commands, and send to provider
func (fp *FromProtocol) HandleWriteAt() error {
	err := fp.waitInitOrCancel()
	if err != nil {
		return err
	}

	var errLock sync.Mutex
	var errValue error

	for {
		// If there was an error in one of the goroutines, return it.
		errLock.Lock()
		if errValue != nil {
			errLock.Unlock()
			return errValue
		}
		errLock.Unlock()

		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandWriteAt)
		if err != nil {
			return err
		}

		if len(data) > 1 && data[1] == packets.WriteAtHash {
			// It could be a WriteAtHash command...
			_, length, _, errWriteAtHash := packets.DecodeWriteAtHash(data)
			if errWriteAtHash != nil {
				return err
			}

			atomic.AddUint64(&fp.metricRecvWriteAtHash, 1)

			// For now, we will simply ack. We do NOT mark it as present. That part will be done when the alternateSources is retrieved.
			// fp.mark_range_present(int(offset), int(length))

			war := &packets.WriteAtResponse{
				Error: nil,
				Bytes: int(length),
			}
			_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeWriteAtResponse(war), UrgencyNormal)
			if err != nil {
				return err
			}
		} else {

			var offset int64
			var writeData []byte

			if len(data) > 1 && data[1] == packets.WriteAtCompRLE {
				offset, writeData, err = packets.DecodeWriteAtComp(data)
				if err == nil {
					atomic.AddUint64(&fp.metricRecvWriteAtComp, 1)
				}
			} else {
				offset, writeData, err = packets.DecodeWriteAt(data)
				if err == nil {
					atomic.AddUint64(&fp.metricRecvWriteAt, 1)
				}
			}
			if err != nil {
				return err
			}

			// Handle in a goroutine
			go func(goffset int64, gdata []byte, gid uint32) {
				n, err := fp.prov.WriteAt(gdata, goffset)
				war := &packets.WriteAtResponse{
					Bytes: n,
					Error: err,
				}
				if err == nil {
					fp.markRangePresent(int(goffset), len(gdata))
				}
				_, err = fp.protocol.SendPacket(fp.dev, gid, packets.EncodeWriteAtResponse(war), UrgencyNormal)
				if err != nil {
					errLock.Lock()
					errValue = err
					errLock.Unlock()
				}
			}(offset, writeData, id)
		}
	}
}

// Handle any WriteAtWithMap commands, and send to provider
func (fp *FromProtocol) HandleWriteAtWithMap(cb func(offset int64, data []byte, idmap map[uint64]uint64) error) error {
	err := fp.waitInitOrCancel()
	if err != nil {
		return err
	}

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandWriteAtWithMap)
		if err != nil {
			return err
		}

		offset, writeData, idMap, err := packets.DecodeWriteAtWithMap(data)
		if err != nil {
			return err
		}

		atomic.AddUint64(&fp.metricRecvWriteAtWithMap, 1)

		err = cb(offset, writeData, idMap)
		if err == nil {
			fp.markRangePresent(int(offset), len(writeData))
		}
		war := &packets.WriteAtResponse{
			Bytes: len(writeData),
			Error: err,
		}
		_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeWriteAtResponse(war), UrgencyNormal)
		if err != nil {
			return err
		}
	}
}

// Handle any RemoveFromMap
func (fp *FromProtocol) HandleRemoveFromMap(cb func(ids []uint64)) error {
	err := fp.waitInitOrCancel()
	if err != nil {
		return err
	}

	for {
		_, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandRemoveFromMap)
		if err != nil {
			return err
		}

		ids, err := packets.DecodeRemoveFromMap(data)
		if err != nil {
			return err
		}

		atomic.AddUint64(&fp.metricRecvRemoveFromMap, 1)

		cb(ids)
		/*
			// TODO: Should probably do this
			if err == nil {
				fp.mark_range_present(int(offset), len(write_data))
			}
		*/
	}
}

// Handle any RemoveDev. Can only trigger once.
func (fp *FromProtocol) HandleRemoveDev(cb func()) error {
	err := fp.waitInitOrCancel()
	if err != nil {
		return err
	}

	_, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandRemoveDev)
	if err != nil {
		return err
	}

	err = packets.DecodeRemoveDev(data)
	if err != nil {
		return err
	}

	atomic.AddUint64(&fp.metricRecvRemoveDev, 1)

	cb()
	return nil
}

// Handle any DirtyList commands
func (fp *FromProtocol) HandleDirtyList(cb func(blocks []uint)) error {
	err := fp.waitInitOrCancel()
	if err != nil {
		return err
	}

	for {
		gid, data, err := fp.protocol.WaitForCommand(fp.dev, packets.CommandDirtyList)
		if err != nil {
			return err
		}
		blockSize, blocks, err := packets.DecodeDirtyList(data)
		if err != nil {
			return err
		}

		atomic.AddUint64(&fp.metricRecvDirtyList, 1)

		// Mark these as non-present (useful for debugging issues)
		for _, b := range blocks {
			offset := int(b) * blockSize
			fp.markRangeMissing(offset, blockSize)
		}

		cb(blocks)

		// Send a response / ack, to signify that the DirtyList has been actioned.
		_, err = fp.protocol.SendPacket(fp.dev, gid, packets.EncodeDirtyListResponse(), UrgencyUrgent)
		if err != nil {
			return err
		}
	}
}

func (fp *FromProtocol) NeedAt(offset int64, length int32) error {
	b := packets.EncodeNeedAt(offset, length)
	_, err := fp.protocol.SendPacket(fp.dev, IDPickAny, b, UrgencyUrgent)
	if err != nil {
		atomic.AddUint64(&fp.metricSentNeedAt, 1)
	}
	return err
}

func (fp *FromProtocol) DontNeedAt(offset int64, length int32) error {
	b := packets.EncodeDontNeedAt(offset, length)
	_, err := fp.protocol.SendPacket(fp.dev, IDPickAny, b, UrgencyUrgent)
	if err != nil {
		atomic.AddUint64(&fp.metricSentDontNeedAt, 1)
	}
	return err
}
