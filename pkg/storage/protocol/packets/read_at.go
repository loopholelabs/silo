package packets

import (
	"encoding/binary"
)

func EncodeReadAt(offset int64, length int32) []byte {
	buff := make([]byte, 1+8+4)
	buff[0] = COMMAND_READ_AT
	binary.LittleEndian.PutUint64(buff[1:], uint64(offset))
	binary.LittleEndian.PutUint32(buff[9:], uint32(length))
	return buff
}

func DecodeReadAt(buff []byte) (int64, int32, error) {
	if buff == nil || len(buff) < 13 || buff[0] != COMMAND_READ_AT {
		return 0, 0, Err_invalid_packet
	}
	off := int64(binary.LittleEndian.Uint64(buff[1:]))
	length := int32(binary.LittleEndian.Uint32(buff[9:]))
	return off, length, nil
}

type ReadAtResponse struct {
	Bytes int
	Data  []byte
	Error error
}

func EncodeReadAtResponse(rar *ReadAtResponse) []byte {
	if rar.Error != nil {
		buff := make([]byte, 1)
		buff[0] = COMMAND_READ_AT_RESPONSE_ERR
		return buff
	} else {
		buff := make([]byte, 1+4+len(rar.Data))
		buff[0] = COMMAND_READ_AT_RESPONSE
		binary.LittleEndian.PutUint32(buff[1:], uint32(rar.Bytes))
		copy(buff[5:], rar.Data)
		return buff
	}
}

func DecodeReadAtResponse(buff []byte) (*ReadAtResponse, error) {
	if buff == nil {
		return nil, Err_invalid_packet
	}

	if buff[0] == COMMAND_READ_AT_RESPONSE_ERR {
		return &ReadAtResponse{
			Error: Err_read_error,
			Bytes: 0,
			Data:  make([]byte, 0),
		}, nil
	} else if buff[0] == COMMAND_READ_AT_RESPONSE {
		if len(buff) < 5 {
			return nil, Err_invalid_packet
		}
		return &ReadAtResponse{
			Error: nil,
			Bytes: int(binary.LittleEndian.Uint32(buff[1:])),
			Data:  buff[5:],
		}, nil
	}

	return nil, Err_invalid_packet
}
