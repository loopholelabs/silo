package modules

import (
	"context"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
)

type sendData struct {
	id   uint32
	data []byte
}

type FromProtocol struct {
	dev        uint32
	prov       storage.StorageProvider
	protocol   protocol.Protocol
	send_queue chan sendData
}

func NewFromProtocol(dev uint32, prov storage.StorageProvider, protocol protocol.Protocol) *FromProtocol {
	return &FromProtocol{
		dev:        dev,
		prov:       prov,
		protocol:   protocol,
		send_queue: make(chan sendData),
	}
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

func (i *FromProtocol) NeedAt(offset int64, length int32) error {
	b := protocol.EncodeNeedAt(offset, length)
	_, err := i.protocol.SendPacket(i.dev, protocol.ID_PICK_ANY, b)
	return err
}
