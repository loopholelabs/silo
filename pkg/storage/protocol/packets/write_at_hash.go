package packets

import (
	"bytes"
	"encoding/binary"
)

type DataLocation byte

const DataLocationS3 = DataLocation(1)
const DataLocationBaseImage = DataLocation(2)

func EncodeWriteAtHash(offset int64, length int64, hash []byte, dataLocation DataLocation) []byte {
	var buff bytes.Buffer

	buff.WriteByte(CommandWriteAt)
	buff.WriteByte(WriteAtHash)
	buff.WriteByte(byte(dataLocation))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(offset))
	buff.Write(b)
	binary.LittleEndian.PutUint64(b, uint64(length))
	buff.Write(b)
	buff.Write(hash)
	return buff.Bytes()
}

func DecodeWriteAtHash(buff []byte) (offset int64, length int64, hash []byte, loc DataLocation, err error) {
	if buff == nil || len(buff) < 19 || buff[0] != CommandWriteAt || buff[1] != WriteAtHash {
		return 0, 0, nil, 0, ErrInvalidPacket
	}
	location := DataLocation(buff[2])
	off := int64(binary.LittleEndian.Uint64(buff[3:]))
	l := int64(binary.LittleEndian.Uint64(buff[11:]))

	return off, l, buff[19:], location, nil
}
