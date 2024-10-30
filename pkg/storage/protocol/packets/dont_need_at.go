package packets

import (
	"encoding/binary"
)

func EncodeDontNeedAt(offset int64, length int32) []byte {
	buff := make([]byte, 1+8+4)
	buff[0] = COMMAND_DONT_NEED_AT
	binary.LittleEndian.PutUint64(buff[1:], uint64(offset))
	binary.LittleEndian.PutUint32(buff[9:], uint32(length))
	return buff
}

func DecodeDontNeedAt(buff []byte) (int64, int32, error) {
	if buff == nil || len(buff) < 13 || buff[0] != COMMAND_DONT_NEED_AT {
		return 0, 0, Err_invalid_packet
	}
	off := int64(binary.LittleEndian.Uint64(buff[1:]))
	length := int32(binary.LittleEndian.Uint32(buff[9:]))
	return off, length, nil
}
