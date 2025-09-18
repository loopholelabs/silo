package protocol

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

const priorityP2P = 2
const priorityAltSources = 1

type StartSyncBehaviour int

const StartSyncASAP = StartSyncBehaviour(1)
const StartSyncDirty = StartSyncBehaviour(2)
const StartSyncCompleted = StartSyncBehaviour(3)

type FromProtocol struct {
	dev             uint32
	prov            storage.Provider
	writeCombinator *modules.WriteCombinator
	provP2P         storage.Provider
	provAltSources  storage.Provider

	providerFactory func(*packets.DevInfo) storage.Provider
	devInfo         *packets.DevInfo
	protocol        Protocol
	init            chan bool
	initLock        sync.Mutex
	ctx             context.Context

	// Collect alternate sources
	alternateSourcesLock sync.Mutex
	alternateSourcesDone bool
	alternateSources     []packets.AlternateSource
	startSyncAt          StartSyncBehaviour

	completeFunc     func()
	completeFuncLock sync.Mutex

	HashWriteHandler func(offset int64, length int64, hash []byte, loc packets.DataLocation, prov storage.Provider)

	// metrics
	metricRecvEvents                uint64
	metricRecvHashes                uint64
	metricRecvDevInfo               uint64
	metricRecvAltSources            uint64
	metricRecvReadAt                uint64
	metricRecvWriteAtHash           uint64
	metricRecvWriteAtYouAlreadyHave uint64
	metricRecvWriteAtComp           uint64
	metricRecvWriteAt               uint64
	metricRecvWriteAtWithMap        uint64
	metricRecvRemoveFromMap         uint64
	metricRecvRemoveDev             uint64
	metricRecvDirtyList             uint64
	metricSentNeedAt                uint64
	metricSentDontNeedAt            uint64
	metricSentReadAt                uint64
	metricSentReadByHash            uint64
}

type FromProtocolMetrics struct {
	RecvEvents                uint64
	RecvHashes                uint64
	RecvDevInfo               uint64
	RecvAltSources            uint64
	RecvReadAt                uint64
	RecvWriteAtHash           uint64
	RecvWriteAtYouAlreadyHave uint64
	RecvWriteAtComp           uint64
	RecvWriteAt               uint64
	RecvWriteAtWithMap        uint64
	RecvRemoveFromMap         uint64
	RecvRemoveDev             uint64
	RecvDirtyList             uint64
	SentNeedAt                uint64
	SentDontNeedAt            uint64
	SentReadAt                uint64
	SentReadByHash            uint64
	WritesAllowedP2P          uint64
	WritesBlockedP2P          uint64
	WritesAllowedAltSources   uint64
	WritesBlockedAltSources   uint64
	NumBlocks                 uint64
	AvailableP2P              []uint
	DuplicateP2P              []uint
	AvailableAltSources       []uint
	DeviceName                string
}

func NewFromProtocol(ctx context.Context, dev uint32, provFactory func(*packets.DevInfo) storage.Provider, protocol Protocol) *FromProtocol {
	return NewFromProtocolWithSyncBehaviour(ctx, dev, provFactory, protocol, StartSyncDirty)
}

func NewFromProtocolWithSyncBehaviour(ctx context.Context, dev uint32, provFactory func(*packets.DevInfo) storage.Provider, protocol Protocol, syncBehaviour StartSyncBehaviour) *FromProtocol {
	fp := &FromProtocol{
		dev:                  dev,
		providerFactory:      provFactory,
		protocol:             protocol,
		init:                 make(chan bool, 1),
		ctx:                  ctx,
		alternateSourcesDone: false,
		startSyncAt:          syncBehaviour,
	}
	return fp
}

func (fp *FromProtocol) GetMetrics() *FromProtocolMetrics {
	fpm := &FromProtocolMetrics{
		RecvEvents:                atomic.LoadUint64(&fp.metricRecvEvents),
		RecvHashes:                atomic.LoadUint64(&fp.metricRecvHashes),
		RecvDevInfo:               atomic.LoadUint64(&fp.metricRecvDevInfo),
		RecvAltSources:            atomic.LoadUint64(&fp.metricRecvAltSources),
		RecvReadAt:                atomic.LoadUint64(&fp.metricRecvReadAt),
		RecvWriteAtHash:           atomic.LoadUint64(&fp.metricRecvWriteAtHash),
		RecvWriteAtYouAlreadyHave: atomic.LoadUint64(&fp.metricRecvWriteAtYouAlreadyHave),
		RecvWriteAtComp:           atomic.LoadUint64(&fp.metricRecvWriteAtComp),
		RecvWriteAt:               atomic.LoadUint64(&fp.metricRecvWriteAt),
		RecvWriteAtWithMap:        atomic.LoadUint64(&fp.metricRecvWriteAtWithMap),
		RecvRemoveFromMap:         atomic.LoadUint64(&fp.metricRecvRemoveFromMap),
		RecvRemoveDev:             atomic.LoadUint64(&fp.metricRecvRemoveDev),
		RecvDirtyList:             atomic.LoadUint64(&fp.metricRecvDirtyList),
		SentNeedAt:                atomic.LoadUint64(&fp.metricSentNeedAt),
		SentDontNeedAt:            atomic.LoadUint64(&fp.metricSentDontNeedAt),
		SentReadAt:                atomic.LoadUint64(&fp.metricSentReadAt),
		SentReadByHash:            atomic.LoadUint64(&fp.metricSentReadByHash),
		WritesAllowedP2P:          0,
		WritesBlockedP2P:          0,
		WritesAllowedAltSources:   0,
		WritesBlockedAltSources:   0,
		NumBlocks:                 0,
		DeviceName:                "",
	}

	fp.initLock.Lock()
	defer fp.initLock.Unlock()
	if fp.devInfo != nil {
		fpm.DeviceName = fp.devInfo.Name
	}

	if fp.writeCombinator != nil {
		met := fp.writeCombinator.GetMetrics()
		fpm.WritesAllowedP2P = met.WritesAllowed[priorityP2P]
		fpm.WritesBlockedP2P = met.WritesBlocked[priorityP2P]
		fpm.WritesAllowedAltSources = met.WritesAllowed[priorityAltSources]
		fpm.WritesBlockedAltSources = met.WritesBlocked[priorityAltSources]
		fpm.AvailableP2P = met.AvailableBlocks[priorityP2P]
		fpm.AvailableAltSources = met.AvailableBlocks[priorityAltSources]
		fpm.DuplicateP2P = met.DuplicatedBlocks[priorityP2P]
		fpm.NumBlocks = uint64(met.NumBlocks)
	}
	return fpm
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

// Get any altSources needed and do the sync.start. Only process ONCE.
func (fp *FromProtocol) getAltSourcesStartSync() {
	fp.alternateSourcesLock.Lock()
	if fp.alternateSourcesDone {
		fp.alternateSourcesLock.Unlock()
		return
	}
	fp.alternateSourcesDone = true

	as := make([]packets.AlternateSource, 0)
	as = append(as, fp.alternateSources...)
	fp.alternateSourcesLock.Unlock()

	// If we got a dirty WriteAt or a localWrite (DontNeedAt sent), then there's wasted work here, but it won't overwrite since we're using
	// WriteCombinator now.

	// Deal with the sync here... We don't wait...
	go func() {
		storage.SendSiloEvent(fp.prov, "sync.start", storage.SyncStartConfig{
			AlternateSources: as,
			Destination:      fp.provAltSources,
		})
		// Notify anyone interested that the S3 grab completed...
		fp.completeFuncLock.Lock()
		if fp.completeFunc != nil {
			fp.completeFunc()
		}
		fp.completeFuncLock.Unlock()
	}()
}

/**
 * Set a function to be called when S3 grab is completed and S3 sync started.
 */
func (fp *FromProtocol) SetCompleteFunc(f func()) {
	fp.completeFuncLock.Lock()
	fp.completeFunc = f
	fp.completeFuncLock.Unlock()
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
			// Start the Sync, if it hasn't been started by dirtyBlocks
			fp.getAltSourcesStartSync()
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

	return fp.SetDevInfo(di)
}

// Alternatively, you can call SetDevInfo to setup the DevInfo.
func (fp *FromProtocol) SetDevInfo(di *packets.DevInfo) error {
	// Create storage, and setup a writeCombinator with two inputs
	fp.initLock.Lock()
	fp.prov = fp.providerFactory(di)
	fp.devInfo = di
	fp.writeCombinator = modules.NewWriteCombinator(fp.prov, int(di.BlockSize))
	fp.provAltSources = fp.writeCombinator.AddSource(priorityAltSources)
	fp.provP2P = fp.writeCombinator.AddSource(priorityP2P)
	fp.initLock.Unlock()

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

			// May want to start sync here
			if fp.startSyncAt == StartSyncASAP {
				fp.getAltSourcesStartSync()
			}
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
			var err error

			buff := make([]byte, glength)
			n, readErr := fp.provP2P.ReadAt(buff, goffset)
			if readErr != nil {
				err = fmt.Errorf("failed to read from offset %d: %w", goffset, readErr)
			}

			rar := &packets.ReadAtResponse{
				Bytes: n,
				Error: err,
				Data:  buff,
			}
			_, sendErr := fp.protocol.SendPacket(fp.dev, gid, packets.EncodeReadAtResponse(rar), UrgencyNormal)
			if sendErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to send ReadAtResponse packet: %w", sendErr))
			}

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

		// WriteAtHash command...
		if len(data) > 1 && data[1] == packets.WriteAtHash {
			offset, length, hash, loc, err := packets.DecodeWriteAtHash(data)
			if err != nil {
				return err
			}

			atomic.AddUint64(&fp.metricRecvWriteAtHash, 1)

			war := &packets.WriteAtResponse{
				Error: nil,
				Bytes: int(length),
			}
			_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeWriteAtResponse(war), UrgencyNormal)
			if err != nil {
				return err
			}
			if fp.HashWriteHandler != nil {
				fp.HashWriteHandler(offset, length, hash, loc, fp.provP2P)
			}
			continue
		}

		// WriteAtYouAlreadyHave command...
		if len(data) > 1 && data[1] == packets.WriteAtYouAlreadyHave {
			blockSize, blocks, err := packets.DecodeYouAlreadyHave(data)
			if err != nil {
				return err
			}

			atomic.AddUint64(&fp.metricRecvWriteAtYouAlreadyHave, 1)

			// Ack
			war := &packets.WriteAtResponse{
				Error: nil,
				Bytes: int(uint64(len(blocks)) * blockSize),
			}
			_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeWriteAtResponse(war), UrgencyNormal)
			if err != nil {
				return err
			}

			for _, b := range blocks {
				offset := int64(uint64(b) * blockSize)
				length := int64(blockSize)
				storage.SendSiloEvent(fp.prov, storage.EventTypeAvailable, storage.EventData([]int64{offset, length}))
			}
			continue
		}

		// Standard WriteAt commands
		var offset int64
		var writeData []byte

		if len(data) > 1 &&
			(data[1] == packets.WriteAtCompRLE || data[1] == packets.WriteAtCompGzip || data[1] == packets.WriteAtCompZeroes) {
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
			var err error

			n, writeErr := fp.provP2P.WriteAt(gdata, goffset)
			if writeErr != nil {
				err = fmt.Errorf("failed to write to offset %d: %w", goffset, writeErr)
			}

			war := &packets.WriteAtResponse{
				Bytes: n,
				Error: err,
			}
			_, sendErr := fp.protocol.SendPacket(fp.dev, gid, packets.EncodeWriteAtResponse(war), UrgencyNormal)
			if sendErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to send WriteAtResponse packet: %w", sendErr))
			}

			if err != nil {
				errLock.Lock()
				errValue = err
				errLock.Unlock()
			}
		}(offset, writeData, id)

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
		_, blocks, err := packets.DecodeDirtyList(data)
		if err != nil {
			return err
		}

		// Once we get a DirtyList, we know we're in the dirty loop sync phase, so we can start sync if it's not already
		// started.
		if fp.startSyncAt == StartSyncDirty {
			fp.getAltSourcesStartSync()
		}

		cb(blocks)

		atomic.AddUint64(&fp.metricRecvDirtyList, 1)

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
	if err == nil {
		atomic.AddUint64(&fp.metricSentNeedAt, 1)
	}
	return err
}

func (fp *FromProtocol) DontNeedAt(offset int64, length int32) error {
	b := packets.EncodeDontNeedAt(offset, length)
	_, err := fp.protocol.SendPacket(fp.dev, IDPickAny, b, UrgencyUrgent)
	if err == nil {
		atomic.AddUint64(&fp.metricSentDontNeedAt, 1)
	}
	return err
}

// Support remote reads
func (fp *FromProtocol) ReadAt(buffer []byte, offset int64) (int, error) {
	b := packets.EncodeReadAt(offset, int32(len(buffer)))
	id, err := fp.protocol.SendPacket(fp.dev, IDPickAny, b, UrgencyUrgent)
	if err != nil {
		return 0, err
	}

	atomic.AddUint64(&fp.metricSentReadAt, 1)

	ret, err := fp.protocol.WaitForPacket(fp.dev, id)
	if err != nil {
		return 0, err
	}
	rar, err := packets.DecodeReadAtResponse(ret)
	if err != nil {
		return 0, err
	}

	// Copy the data into the provided buffer
	if rar.Data != nil {
		copy(buffer, rar.Data)
	}
	return rar.Bytes, rar.Error
}

// Support remote reads by hash
func (fp *FromProtocol) ReadByHash(hash [sha256.Size]byte) ([]byte, error) {
	b := packets.EncodeReadByHash(hash)
	id, err := fp.protocol.SendPacket(fp.dev, IDPickAny, b, UrgencyUrgent)
	if err != nil {
		return nil, err
	}

	atomic.AddUint64(&fp.metricSentReadByHash, 1)

	ret, err := fp.protocol.WaitForPacket(fp.dev, id)
	if err != nil {
		return nil, err
	}
	rar, err := packets.DecodeReadByHashResponse(ret)
	if err != nil {
		return nil, err
	}

	return rar.Data, rar.Error
}
