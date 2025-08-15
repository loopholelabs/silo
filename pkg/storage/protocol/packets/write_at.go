package packets

import (
	"encoding/binary"
)

const WriteAtData = 0
const WriteAtHash = 1
const WriteAtYouAlreadyHave = 3

const WriteAtCompRLE = 2
const WriteAtCompGzip = 4
const WriteAtCompZeroes = 5

func WriteAtType(writeType byte) string {
	switch writeType {
	case WriteAtData:
		return "WriteAt"
	case WriteAtHash:
		return "WriteAtHash"
	case WriteAtYouAlreadyHave:
		return "WriteAtYouAlreadyHave"
	case WriteAtCompRLE:
		return "WriteAtCompRLE"
	case WriteAtCompGzip:
		return "WriteAtCompGzip"
	case WriteAtCompZeroes:
		return "WriteAtCompZeroes"
	}
	return "unknown"
}

func EncodeWriteAt(offset int64, data []byte) []byte {
	buff := make([]byte, 2+8+len(data))
	buff[0] = CommandWriteAt
	buff[1] = WriteAtData
	binary.LittleEndian.PutUint64(buff[2:], uint64(offset))
	copy(buff[10:], data)
	return buff
}

func DecodeWriteAt(buff []byte) (offset int64, data []byte, err error) {
	if len(buff) < 10 || buff[0] != CommandWriteAt || buff[1] != WriteAtData {
		return 0, nil, ErrInvalidPacket
	}
	off := int64(binary.LittleEndian.Uint64(buff[2:]))
	return off, buff[10:], nil
}

type WriteAtResponse struct {
	Bytes int
	Error error
}

func EncodeWriteAtResponse(war *WriteAtResponse) []byte {
	if war.Error != nil {
		buff := make([]byte, 1)
		buff[0] = CommandWriteAtResponseErr
		return buff
	}
	buff := make([]byte, 1+4)
	buff[0] = CommandWriteAtResponse
	binary.LittleEndian.PutUint32(buff[1:], uint32(war.Bytes))
	return buff
}

func DecodeWriteAtResponse(buff []byte) (*WriteAtResponse, error) {
	if buff == nil {
		return nil, ErrInvalidPacket
	}
	if buff[0] == CommandWriteAtResponseErr {
		return &WriteAtResponse{
			Error: ErrWriteError,
			Bytes: 0,
		}, nil
	} else if buff[0] == CommandWriteAtResponse {
		if len(buff) < 5 {
			return nil, ErrInvalidPacket
		}
		return &WriteAtResponse{
			Error: nil,
			Bytes: int(binary.LittleEndian.Uint32(buff[1:])),
		}, nil
	}
	return nil, ErrInvalidPacket
}
