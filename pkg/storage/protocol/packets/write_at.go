package packets

import (
	"encoding/binary"
	"errors"
	"io"
)

const WRITE_AT_DATA = 0
const WRITE_AT_HASH = 1
const WRITE_AT_COMP_RLE = 2

func EncodeWriteAt(offset int64, data []byte) []byte {
	buff := make([]byte, 2+8+len(data))
	buff[0] = COMMAND_WRITE_AT
	buff[1] = WRITE_AT_DATA
	binary.LittleEndian.PutUint64(buff[2:], uint64(offset))
	copy(buff[10:], data)
	return buff
}

func EncodeWriterWriteAt(offset int64, data []byte) (uint32, func(w io.Writer) error) {
	return uint32(10 + len(data)), func(w io.Writer) error {
		header := make([]byte, 2+8)
		header[0] = COMMAND_WRITE_AT
		header[1] = WRITE_AT_DATA
		binary.LittleEndian.PutUint64(header[2:], uint64(offset))
		_, err := w.Write(header)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	}
}

func DecodeWriteAt(buff []byte) (offset int64, data []byte, err error) {
	if buff == nil || len(buff) < 10 || buff[0] != COMMAND_WRITE_AT || buff[1] != WRITE_AT_DATA {
		return 0, nil, errors.New("Invalid packet command")
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
