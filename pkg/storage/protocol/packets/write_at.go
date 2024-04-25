package packets

import (
	"encoding/binary"
	"errors"
	"io"
)

func EncodeWriteAt(offset int64, data []byte) []byte {
	buff := make([]byte, 1+8+len(data))
	buff[0] = COMMAND_WRITE_AT
	binary.LittleEndian.PutUint64(buff[1:], uint64(offset))
	copy(buff[9:], data)
	return buff
}

func EncodeWriterWriteAt(offset int64, data []byte) (uint32, func(w io.Writer) error) {
	return uint32(9 + len(data)), func(w io.Writer) error {
		header := make([]byte, 1+8)
		header[0] = COMMAND_WRITE_AT
		binary.LittleEndian.PutUint64(header[1:], uint64(offset))
		_, err := w.Write(header)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	}
}

func DecodeWriteAt(buff []byte) (offset int64, data []byte, err error) {
	if buff == nil || len(buff) < 9 || buff[0] != COMMAND_WRITE_AT {
		return 0, nil, errors.New("Invalid packet command")
	}
	off := int64(binary.LittleEndian.Uint64(buff[1:]))
	return off, buff[9:], nil
}

type WriteAtResponse struct {
	Bytes int
	Error error
}

func EncodeWriteAtResponse(war *WriteAtResponse) []byte {
	if war.Error != nil {
		buff := make([]byte, 1)
		buff[0] = COMMAND_WRITE_AT_RESPONSE_ERR
		return buff
	} else {
		buff := make([]byte, 1+4)
		buff[0] = COMMAND_WRITE_AT_RESPONSE
		binary.LittleEndian.PutUint32(buff[1:], uint32(war.Bytes))
		return buff
	}
}

func DecodeWriteAtResponse(buff []byte) (*WriteAtResponse, error) {
	if buff == nil {
		return nil, errors.New("Invalid packet")
	}
	if buff[0] == COMMAND_WRITE_AT_RESPONSE_ERR {
		return &WriteAtResponse{
			Error: errors.New("Remote error"),
			Bytes: 0,
		}, nil
	} else if buff[0] == COMMAND_WRITE_AT_RESPONSE {
		if len(buff) < 5 {
			return nil, errors.New("Invalid packet")
		}
		return &WriteAtResponse{
			Error: nil,
			Bytes: int(binary.LittleEndian.Uint32(buff[1:])),
		}, nil
	}
	return nil, errors.New("Unknown packet")
}
