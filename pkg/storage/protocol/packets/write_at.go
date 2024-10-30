package packets

import (
	"encoding/binary"
	"io"
)

const WriteAtData = 0
const WriteAtHash = 1
const WriteAtCompRLE = 2

func EncodeWriteAt(offset int64, data []byte) []byte {
	buff := make([]byte, 2+8+len(data))
	buff[0] = CommandWriteAt
	buff[1] = WriteAtData
	binary.LittleEndian.PutUint64(buff[2:], uint64(offset))
	copy(buff[10:], data)
	return buff
}

func EncodeWriterWriteAt(offset int64, data []byte) (uint32, func(w io.Writer) error) {
	return uint32(10 + len(data)), func(w io.Writer) error {
		header := make([]byte, 2+8)
		header[0] = CommandWriteAt
		header[1] = WriteAtData
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
	if buff == nil || len(buff) < 10 || buff[0] != CommandWriteAt || buff[1] != WriteAtData {
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
