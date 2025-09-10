package packets

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

func EncodeReadByHash(hash [sha256.Size]byte) []byte {
	buff := make([]byte, 1+sha256.Size)
	buff[0] = CommandReadByHash
	copy(buff[1:], hash[:])
	return buff
}

func DecodeReadByHash(buff []byte) ([]byte, error) {
	if len(buff) < 1+sha256.Size || buff[0] != CommandReadByHash {
		return nil, ErrInvalidPacket
	}
	hash := make([]byte, sha256.Size)
	copy(hash, buff[1:])
	return hash, nil
}

type ReadByHashResponse struct {
	Data  []byte
	Error error
}

func EncodeReadByHashResponse(rar *ReadByHashResponse) []byte {
	if rar.Error != nil {
		errString := rar.Error.Error()
		buff := make([]byte, 1+4+len(errString))
		buff[0] = CommandReadByHashResponseErr
		binary.LittleEndian.PutUint32(buff[1:], uint32(len(errString)))
		copy(buff[5:], errString)
		return buff
	}
	buff := make([]byte, 1+4+len(rar.Data))
	buff[0] = CommandReadByHashResponse
	binary.LittleEndian.PutUint32(buff[1:], uint32(len(rar.Data)))
	copy(buff[5:], rar.Data)
	return buff
}

func DecodeReadByHashResponse(buff []byte) (*ReadByHashResponse, error) {
	if buff == nil {
		return nil, ErrInvalidPacket
	}

	switch buff[0] {
	case CommandReadByHashResponseErr:
		errLen := binary.LittleEndian.Uint32(buff[1:])
		return &ReadByHashResponse{
			Error: errors.New(string(buff[5 : 5+errLen])),
			Data:  make([]byte, 0),
		}, nil
	case CommandReadByHashResponse:
		if len(buff) < 5 {
			return nil, ErrInvalidPacket
		}
		length := int(binary.LittleEndian.Uint32(buff[1:]))
		if len(buff) < 5+length {
			return nil, ErrInvalidPacket
		}
		return &ReadByHashResponse{
			Error: nil,
			Data:  buff[5 : 5+length],
		}, nil
	}

	return nil, ErrInvalidPacket
}
