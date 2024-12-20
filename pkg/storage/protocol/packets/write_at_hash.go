package packets

import (
	"bytes"
	"encoding/binary"
)

func EncodeWriteAtHash(offset int64, length int64, hash []byte) []byte {
	var buff bytes.Buffer

	buff.WriteByte(CommandWriteAt)
	buff.WriteByte(WriteAtHash)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(offset))
	buff.Write(b)
	binary.LittleEndian.PutUint64(b, uint64(length))
	buff.Write(b)
	buff.Write(hash)
	return buff.Bytes()
}

func DecodeWriteAtHash(buff []byte) (offset int64, length int64, hash []byte, err error) {
	if buff == nil || len(buff) < 18 || buff[0] != CommandWriteAt || buff[1] != WriteAtHash {
		return 0, 0, nil, ErrInvalidPacket
	}
	off := int64(binary.LittleEndian.Uint64(buff[2:]))
	l := int64(binary.LittleEndian.Uint64(buff[10:]))

	return off, l, buff[18:], nil
}
