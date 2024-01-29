package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
)

type FromProtocol struct {
	dev      uint32
	prov     storage.StorageProvider
	protocol protocol.Protocol
}

func NewFromProtocol(dev uint32, prov storage.StorageProvider, protocol protocol.Protocol) *FromProtocol {
	return &FromProtocol{
		dev:      dev,
		prov:     prov,
		protocol: protocol,
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
		buff := make([]byte, length)
		n, err := fp.prov.ReadAt(buff, offset)
		rar := &protocol.ReadAtResponse{
			Bytes: n,
			Error: err,
			Data:  buff,
		}
		_, err = fp.protocol.SendPacket(fp.dev, id, protocol.EncodeReadAtResponse(rar))
		if err != nil {
			return err
		}
	}
}

// Handle any WriteAt commands, and send to provider
func (fp *FromProtocol) HandleWriteAt() error {
	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, protocol.COMMAND_WRITE_AT)
		if err != nil {
			return err
		}
		offset, data, err := protocol.DecodeWriteAt(data)
		if err != nil {
			return err
		}
		n, err := fp.prov.WriteAt(data, offset)
		war := &protocol.WriteAtResponse{
			Bytes: n,
			Error: err,
		}
		_, err = fp.protocol.SendPacket(fp.dev, id, protocol.EncodeWriteAtResponse(war))
		if err != nil {
			return err
		}
	}
}
