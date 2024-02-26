package modules

import (
	"context"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
)

type sendData struct {
	id   uint32
	data []byte
}

type FromProtocol struct {
	dev         uint32
	prov        storage.StorageProvider
	provFactory func(*protocol.DevInfo) storage.StorageProvider
	protocol    protocol.Protocol
	send_queue  chan sendData
	init        sync.WaitGroup
}

func NewFromProtocol(dev uint32, provFactory func(*protocol.DevInfo) storage.StorageProvider, protocol protocol.Protocol) *FromProtocol {
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

// Handle a DevInfo, and create the storage
func (fp *FromProtocol) HandleDevInfo() error {
	_, data, err := fp.protocol.WaitForCommand(fp.dev, protocol.COMMAND_DEV_INFO)
	if err != nil {
		return err
	}
	di, err := protocol.DecodeDevInfo(data)
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
		id, data, err := fp.protocol.WaitForCommand(fp.dev, protocol.COMMAND_READ_AT)
		if err != nil {
			return err
		}
		offset, length, err := protocol.DecodeReadAt(data)
		if err != nil {
			return err
		}

		// Handle them in goroutines
		go func(goffset int64, glength int32, gid uint32) {
			buff := make([]byte, glength)
			n, err := fp.prov.ReadAt(buff, goffset)
			rar := &protocol.ReadAtResponse{
				Bytes: n,
				Error: err,
				Data:  buff,
			}
			fp.send_queue <- sendData{
				id:   gid,
				data: protocol.EncodeReadAtResponse(rar),
			}
		}(offset, length, id)
	}
}

// Handle any WriteAt commands, and send to provider
func (fp *FromProtocol) HandleWriteAt() error {
	fp.init.Wait()

	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, protocol.COMMAND_WRITE_AT)
		if err != nil {
			return err
		}

		offset, write_data, err := protocol.DecodeWriteAt(data)
		if err != nil {
			return err
		}

		// Handle in a goroutine
		go func(goffset int64, gdata []byte, gid uint32) {
			n, err := fp.prov.WriteAt(gdata, goffset)
			war := &protocol.WriteAtResponse{
				Bytes: n,
				Error: err,
			}
			fp.send_queue <- sendData{
				id:   gid,
				data: protocol.EncodeWriteAtResponse(war),
			}
		}(offset, write_data, id)
	}
}

// Handle any DirtyList commands
func (fp *FromProtocol) HandleDirtyList(cb func(blocks []uint)) error {
	for {
		_, data, err := fp.protocol.WaitForCommand(fp.dev, protocol.COMMAND_DIRTY_LIST)
		if err != nil {
			return err
		}
		blocks, err := protocol.DecodeDirtyList(data)
		if err != nil {
			return err
		}

		cb(blocks)
	}
}

func (i *FromProtocol) NeedAt(offset int64, length int32) error {
	b := protocol.EncodeNeedAt(offset, length)
	_, err := i.protocol.SendPacket(i.dev, protocol.ID_PICK_ANY, b)
	return err
}

func (i *FromProtocol) DontNeedAt(offset int64, length int32) error {
	b := protocol.EncodeDontNeedAt(offset, length)
	_, err := i.protocol.SendPacket(i.dev, protocol.ID_PICK_ANY, b)
	return err
}
