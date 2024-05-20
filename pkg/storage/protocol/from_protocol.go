package protocol

import (
	"crypto/sha256"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

type FromProtocol struct {
	dev          uint32
	prov         storage.StorageProvider
	prov_factory func(*packets.DevInfo) storage.StorageProvider
	protocol     Protocol
	init         sync.WaitGroup
}

func NewFromProtocol(dev uint32, provFactory func(*packets.DevInfo) storage.StorageProvider, protocol Protocol) *FromProtocol {
	fp := &FromProtocol{
		dev:          dev,
		prov_factory: provFactory,
		protocol:     protocol,
	}
	// We need to wait for the DevInfo before allowing any reads/writes.
	fp.init.Add(1)
	return fp
}

// Handle any Events
func (fp *FromProtocol) HandleEvent(cb func(*packets.Event)) error {
	fp.init.Wait()

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
	fp.init.Wait()

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
	fp.init.Done() // Allow reads/writes
	return nil
}

// Handle any ReadAt commands, and send to provider
func (fp *FromProtocol) HandleReadAt() error {
	fp.init.Wait()

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
	fp.init.Wait()

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
	fp.init.Wait()

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

// Handle any WriteAtComp commands, and send to provider
func (fp *FromProtocol) HandleWriteAtComp() error {
	fp.init.Wait()

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
	for {
		_, data, err := fp.protocol.WaitForCommand(fp.dev, packets.COMMAND_DIRTY_LIST)
		if err != nil {
			return err
		}
		blocks, err := packets.DecodeDirtyList(data)
		if err != nil {
			return err
		}

		cb(blocks)
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
