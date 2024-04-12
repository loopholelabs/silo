package protocol

import (
	"context"
	"crypto/sha256"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

type sendData struct {
	id   uint32
	data []byte
}

type FromProtocol struct {
	dev         uint32
	prov        storage.StorageProvider
	provFactory func(*DevInfo) storage.StorageProvider
	protocol    Protocol
	send_queue  chan sendData
	init        sync.WaitGroup
}

func NewFromProtocol(dev uint32, provFactory func(*DevInfo) storage.StorageProvider, protocol Protocol) *FromProtocol {
	fp := &FromProtocol{
		dev:         dev,
		provFactory: provFactory,
		protocol:    protocol,
		send_queue:  make(chan sendData),
	}
	// We need to wait for the DevInfo before allowing any reads/writes.
	fp.init.Add(1)
	return fp
}

// Handle any Events
func (fp *FromProtocol) HandleEvent(cb func(*Event)) error {
	fp.init.Wait()

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, COMMAND_EVENT)
		if err != nil {
			return err
		}
		ev, err := DecodeEvent(data)
		if err != nil {
			return err
		}

		// Relay the event, wait, and then respond.
		cb(ev)

		fp.send_queue <- sendData{
			id:   id,
			data: EncodeEventResponse(),
		}
	}
}

// Handle hashes
func (fp *FromProtocol) HandleHashes(cb func(map[uint][sha256.Size]byte)) error {
	fp.init.Wait()

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, COMMAND_HASHES)
		if err != nil {
			return err
		}
		hashes, err := DecodeHashes(data)
		if err != nil {
			return err
		}

		// Relay the hashes, wait and then respond
		cb(hashes)

		fp.send_queue <- sendData{
			id:   id,
			data: EncodeHashesResponse(),
		}
	}
}

// Handle a DevInfo, and create the storage
func (fp *FromProtocol) HandleDevInfo() error {
	_, data, err := fp.protocol.WaitForCommand(fp.dev, COMMAND_DEV_INFO)
	if err != nil {
		return err
	}
	di, err := DecodeDevInfo(data)
	if err != nil {
		return err
	}

	// Create storage
	fp.prov = fp.provFactory(di)
	fp.init.Done() // Allow reads/writes
	return nil
}

// Send packets out
func (fp *FromProtocol) HandleSend(ctx context.Context) error {
	for {
		select {
		case s := <-fp.send_queue:
			_, err := fp.protocol.SendPacket(fp.dev, s.id, s.data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// Handle any ReadAt commands, and send to provider
func (fp *FromProtocol) HandleReadAt() error {
	fp.init.Wait()

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, COMMAND_READ_AT)
		if err != nil {
			return err
		}
		offset, length, err := DecodeReadAt(data)
		if err != nil {
			return err
		}

		// Handle them in goroutines
		go func(goffset int64, glength int32, gid uint32) {
			buff := make([]byte, glength)
			n, err := fp.prov.ReadAt(buff, goffset)
			rar := &ReadAtResponse{
				Bytes: n,
				Error: err,
				Data:  buff,
			}
			fp.send_queue <- sendData{
				id:   gid,
				data: EncodeReadAtResponse(rar),
			}
		}(offset, length, id)
	}
}

// Handle any WriteAt commands, and send to provider
func (fp *FromProtocol) HandleWriteAt() error {
	fp.init.Wait()

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, COMMAND_WRITE_AT)
		if err != nil {
			return err
		}

		offset, write_data, err := DecodeWriteAt(data)
		if err != nil {
			return err
		}

		// Handle in a goroutine
		go func(goffset int64, gdata []byte, gid uint32) {
			n, err := fp.prov.WriteAt(gdata, goffset)
			war := &WriteAtResponse{
				Bytes: n,
				Error: err,
			}
			fp.send_queue <- sendData{
				id:   gid,
				data: EncodeWriteAtResponse(war),
			}
		}(offset, write_data, id)
	}
}

// Handle any WriteAtComp commands, and send to provider
func (fp *FromProtocol) HandleWriteAtComp() error {
	fp.init.Wait()

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, COMMAND_WRITE_AT_COMP)
		if err != nil {
			return err
		}

		offset, write_data, err := DecodeWriteAtComp(data)
		if err != nil {
			return err
		}

		// Handle in a goroutine
		go func(goffset int64, gdata []byte, gid uint32) {
			n, err := fp.prov.WriteAt(gdata, goffset)
			war := &WriteAtResponse{
				Bytes: n,
				Error: err,
			}
			fp.send_queue <- sendData{
				id:   gid,
				data: EncodeWriteAtResponse(war),
			}
		}(offset, write_data, id)
	}
}

// Handle any DirtyList commands
func (fp *FromProtocol) HandleDirtyList(cb func(blocks []uint)) error {
	for {
		_, data, err := fp.protocol.WaitForCommand(fp.dev, COMMAND_DIRTY_LIST)
		if err != nil {
			return err
		}
		blocks, err := DecodeDirtyList(data)
		if err != nil {
			return err
		}

		cb(blocks)
	}
}

func (i *FromProtocol) NeedAt(offset int64, length int32) error {
	b := EncodeNeedAt(offset, length)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}

func (i *FromProtocol) DontNeedAt(offset int64, length int32) error {
	b := EncodeDontNeedAt(offset, length)
	_, err := i.protocol.SendPacket(i.dev, ID_PICK_ANY, b)
	return err
}
