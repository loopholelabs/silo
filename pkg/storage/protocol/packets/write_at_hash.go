package packets

import (
	"bytes"
	"encoding/binary"
	"errors"
)

func EncodeWriteAtHash(offset int64, length int64, hash []byte) []byte {
	var buff bytes.Buffer

	buff.WriteByte(COMMAND_WRITE_AT_HASH)
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, uint64(offset))
	buff.Write(b)
	binary.LittleEndian.PutUint64(b, uint64(length))
	buff.Write(b)
	buff.Write(hash)

	return buff.Bytes()
}

func DecodeWriteAtHash(buff []byte) (offset int64, length int64, hash []byte, err error) {
	if buff == nil || len(buff) < 17 || buff[0] != COMMAND_WRITE_AT_HASH {
		return 0, 0, nil, errors.New("invalid packet command")
	}
	off := int64(binary.LittleEndian.Uint64(buff[1:]))
	l := int64(binary.LittleEndian.Uint64(buff[9:]))

	return off, l, buff[17:], nil
}
