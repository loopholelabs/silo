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

		// Handle them in goroutines
		go func(gdata []byte, gid uint32) {
			offset, length, err := protocol.DecodeReadAt(gdata)
			if err != nil {
				// TODO: Report this to somewhere
			}
			buff := make([]byte, length)
			n, err := fp.prov.ReadAt(buff, offset)
			rar := &protocol.ReadAtResponse{
				Bytes: n,
				Error: err,
				Data:  buff,
			}
			_, err = fp.protocol.SendPacket(fp.dev, gid, protocol.EncodeReadAtResponse(rar))
			if err != nil {
				// TODO: Report this to somewhere
			}
		}(data, id)
	}
}

// Handle any WriteAt commands, and send to provider
func (fp *FromProtocol) HandleWriteAt() error {
	for {
		id, data, err := fp.protocol.WaitForCommand(fp.dev, protocol.COMMAND_WRITE_AT)
		if err != nil {
			return err
		}

		// Handle in a goroutine
		go func(gdata []byte, gid uint32) {
			offset, data, err := protocol.DecodeWriteAt(gdata)
			if err != nil {
				// TODO
			}
			n, err := fp.prov.WriteAt(data, offset)
			war := &protocol.WriteAtResponse{
				Bytes: n,
				Error: err,
			}
			_, err = fp.protocol.SendPacket(fp.dev, gid, protocol.EncodeWriteAtResponse(war))
			if err != nil {
				// TODO
			}
		}(data, id)
	}
}
