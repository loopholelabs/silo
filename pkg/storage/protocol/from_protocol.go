package protocol

import (
	"context"
	"crypto/sha256"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type FromProtocol struct {
	dev          uint32
	prov         storage.StorageProvider
	prov_factory func(*packets.DevInfo) storage.StorageProvider
	protocol     Protocol
	init         chan bool
	ctx          context.Context

	present_lock       sync.Mutex
	present            *util.Bitfield
	present_block_size int
}

func NewFromProtocol(ctx context.Context, dev uint32, provFactory func(*packets.DevInfo) storage.StorageProvider, protocol Protocol) *FromProtocol {
	fp := &FromProtocol{
		dev:                dev,
		prov_factory:       provFactory,
		protocol:           protocol,
		present_block_size: 1024,
		init:               make(chan bool, 1),
		ctx:                ctx,
	}
	return fp
}

func (fp *FromProtocol) GetDataPresent() int {
	fp.present_lock.Lock()
	defer fp.present_lock.Unlock()
	blocks := fp.present.Count(0, fp.present.Length())
	size := blocks * fp.present_block_size
	if size > int(fp.prov.Size()) {
		size = int(fp.prov.Size())
	}
	return size
}

func (fp *FromProtocol) mark_range_present(offset int, length int) {
	// Translate to full blocks...
	end := uint64(offset + length)
	if end > fp.prov.Size() {
		end = fp.prov.Size()
	}

	b_start := uint(offset / fp.present_block_size)
	b_end := uint((end-1)/uint64(fp.present_block_size)) + 1

	// First block is incomplete. Ignore it.
	if offset > (int(b_start) * fp.present_block_size) {
		b_start++
	}

	// Last block is incomplete AND is not the last block. Ignore it.
	if offset+length < int(b_end*uint(fp.present_block_size)) && offset+length < int(fp.prov.Size()) {
		b_end--
	}

	// Mark the blocks
	fp.present_lock.Lock()
	fp.present.SetBits(b_start, b_end)
	fp.present_lock.Unlock()
}

func (fp *FromProtocol) mark_range_missing(offset int, length int) {
	// Translate to full blocks...
	end := uint64(offset + length)
	if end > fp.prov.Size() {
		end = fp.prov.Size()
	}

	b_start := uint(offset / fp.present_block_size)
	b_end := uint((end-1)/uint64(fp.present_block_size)) + 1

	// First block is incomplete. Ignore it.
	if offset > (int(b_start) * fp.present_block_size) {
		b_start++
	}

	// Last block is incomplete AND is not the last block. Ignore it.
	if offset+length < int(b_end*uint(fp.present_block_size)) && offset+length < int(fp.prov.Size()) {
		b_end--
	}

	// Mark the blocks
	fp.present_lock.Lock()
	fp.present.ClearBits(b_start, b_end)
	fp.present_lock.Unlock()
}

func (fp *FromProtocol) wait_init_or_cancel() error {
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
	err := fp.wait_init_or_cancel()
	if err != nil {
		return err
	}

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_EVENT)
		if err != nil {
			return err
		}
		ev, err := packets.DecodeEvent(data)
		if err != nil {
			return err
		}

		// Relay the event, wait, and then respond.
		cb(ev)

		_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeEventResponse())
		if err != nil {
			return err
		}
	}
}

// Handle hashes
func (fp *FromProtocol) HandleHashes(cb func(map[uint][sha256.Size]byte)) error {
	err := fp.wait_init_or_cancel()
	if err != nil {
		return err
	}

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_HASHES)
		if err != nil {
			return err
		}
		hashes, err := packets.DecodeHashes(data)
		if err != nil {
			return err
		}

		// Relay the hashes, wait and then respond
		cb(hashes)

		_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeHashesResponse())
		if err != nil {
			return err
		}
	}
}

// Handle a DevInfo, and create the storage
func (fp *FromProtocol) HandleDevInfo() error {
	_, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_DEV_INFO)
	if err != nil {
		return err
	}
	di, err := packets.DecodeDevInfo(data)
	if err != nil {
		return err
	}

	// Create storage
	fp.prov = fp.prov_factory(di)
	num_blocks := (int(fp.prov.Size()) + fp.present_block_size - 1) / fp.present_block_size
	fp.present = util.NewBitfield(num_blocks)

	fp.init <- true // Confirm things have been initialized for this device.
	return nil
}

// Handle any ReadAt commands, and send to provider
func (fp *FromProtocol) HandleReadAt() error {
	err := fp.wait_init_or_cancel()
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

		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_READ_AT)
		if err != nil {
			return err
		}
		offset, length, err := packets.DecodeReadAt(data)
		if err != nil {
			return err
		}

		// Handle them in goroutines
		go func(goffset int64, glength int32, gid uint32) {
			buff := make([]byte, glength)
			n, err := fp.prov.ReadAt(buff, goffset)
			rar := &packets.ReadAtResponse{
				Bytes: n,
				Error: err,
				Data:  buff,
			}
			_, err = fp.protocol.SendPacket(fp.dev, gid, packets.EncodeReadAtResponse(rar))
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
	err := fp.wait_init_or_cancel()
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

		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_WRITE_AT)
		if err != nil {
			return err
		}

		offset, write_data, err := packets.DecodeWriteAt(data)
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
				fp.mark_range_present(int(goffset), len(gdata))
			}
			_, err = fp.protocol.SendPacket(fp.dev, gid, packets.EncodeWriteAtResponse(war))
			if err != nil {
				errLock.Lock()
				errValue = err
				errLock.Unlock()
			}
		}(offset, write_data, id)
	}
}

// Handle any WriteAt commands, and send to provider
func (fp *FromProtocol) HandleWriteAtWithMap(cb func(offset int64, data []byte, idmap map[uint64]uint64) error) error {
	err := fp.wait_init_or_cancel()
	if err != nil {
		return err
	}

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_WRITE_AT_WITH_MAP)
		if err != nil {
			return err
		}

		offset, write_data, id_map, err := packets.DecodeWriteAtWithMap(data)
		if err != nil {
			return err
		}

		err = cb(offset, write_data, id_map)
		if err == nil {
			fp.mark_range_present(int(offset), len(write_data))
		}
		war := &packets.WriteAtResponse{
			Bytes: len(write_data),
			Error: err,
		}
		_, err = fp.protocol.SendPacket(fp.dev, id, packets.EncodeWriteAtResponse(war))
		if err != nil {
			return err
		}
	}
}

// Handle any RemoveFromMap
func (fp *FromProtocol) HandleRemoveFromMap(cb func(ids []uint64)) error {
	err := fp.wait_init_or_cancel()
	if err != nil {
		return err
	}

	for {
		_, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_REMOVE_FROM_MAP)
		if err != nil {
			return err
		}

		ids, err := packets.DecodeRemoveFromMap(data)
		if err != nil {
			return err
		}

		cb(ids)
		/*
			// Should probably do this
			if err == nil {
				fp.mark_range_present(int(offset), len(write_data))
			}
		*/
	}
}

// Handle any RemoveDev. Can only trigger once.
func (fp *FromProtocol) HandleRemoveDev(cb func()) error {
	err := fp.wait_init_or_cancel()
	if err != nil {
		return err
	}

	_, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_REMOVE_DEV)
	if err != nil {
		return err
	}

	err = packets.DecodeRemoveDev(data)
	if err != nil {
		return err
	}

	cb()
	return nil
}

// Handle any WriteAtComp commands, and send to provider
func (fp *FromProtocol) HandleWriteAtComp() error {
	err := fp.wait_init_or_cancel()
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

		id, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_WRITE_AT_COMP)
		if err != nil {
			return err
		}

		offset, write_data, err := packets.DecodeWriteAtComp(data)
		if err != nil {
			return err
		}

		// Handle in a goroutine
		go func(goffset int64, gdata []byte, gid uint32) {
			n, err := fp.prov.WriteAt(gdata, goffset)
			if err == nil {
				fp.mark_range_present(int(goffset), len(gdata))
			}
			war := &packets.WriteAtResponse{
				Bytes: n,
				Error: err,
			}
			_, err = fp.protocol.SendPacket(fp.dev, gid, packets.EncodeWriteAtResponse(war))
			if err != nil {
				errLock.Lock()
				errValue = err
				errLock.Unlock()
			}
		}(offset, write_data, id)
	}
}

// Handle any DirtyList commands
func (fp *FromProtocol) HandleDirtyList(cb func(blocks []uint)) error {
	err := fp.wait_init_or_cancel()
	if err != nil {
		return err
	}

	for {
		gid, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_DIRTY_LIST)
		if err != nil {
			return err
		}
		block_size, blocks, err := packets.DecodeDirtyList(data)
		if err != nil {
			return err
		}

		// Mark these as non-present (useful for debugging issues)
		for _, b := range blocks {
			offset := int(b) * block_size
			fp.mark_range_missing(offset, block_size)
		}

		cb(blocks)

		// Send a response / ack, to signify that the DirtyList has been actioned.
		_, err = fp.protocol.SendPacket(fp.dev, gid, packets.EncodeDirtyListResponse())
		if err != nil {
			return err
		}
	}
}

func (i *FromProtocol) NeedAt(offset int64, length int32) error {
	b := packets.EncodeNeedAt(offset, length)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}

func (i *FromProtocol) DontNeedAt(offset int64, length int32) error {
	b := packets.EncodeDontNeedAt(offset, length)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}
