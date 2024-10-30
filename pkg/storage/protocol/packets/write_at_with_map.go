package packets

import (
	"encoding/binary"
)

func EncodeWriteAtWithMap(offset int64, data []byte, idmap map[uint64]uint64) []byte {
	buff := make([]byte, 1+8+4+(len(idmap)*16)+len(data))
	buff[0] = COMMAND_WRITE_AT_WITH_MAP
	binary.LittleEndian.PutUint64(buff[1:], uint64(offset))
	binary.LittleEndian.PutUint32(buff[9:], uint32(len(idmap)))

	// Encode the map here...
	ptr := 13
	for k, v := range idmap {
		binary.LittleEndian.PutUint64(buff[ptr:], k)
		binary.LittleEndian.PutUint64(buff[ptr+8:], v)
		ptr += 16
	}

	copy(buff[ptr:], data)
	return buff
}

func DecodeWriteAtWithMap(buff []byte) (int64, []byte, map[uint64]uint64, error) {
	if buff == nil || len(buff) < 13 || buff[0] != COMMAND_WRITE_AT_WITH_MAP {
		return 0, nil, nil, ErrInvalidPacket
	}
	off := int64(binary.LittleEndian.Uint64(buff[1:]))
	idlen := binary.LittleEndian.Uint32(buff[9:])
	idmap := make(map[uint64]uint64)

	ptr := 13
	for i := 0; i < int(idlen); i++ {
		k := binary.LittleEndian.Uint64(buff[ptr:])
		v := binary.LittleEndian.Uint64(buff[ptr+8:])
		idmap[k] = v
		ptr += 16
	}
	return off, buff[ptr:], idmap, nil
}
