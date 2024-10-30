package packets

import (
	"encoding/binary"
)

func EncodeRemoveFromMap(ids []uint64) []byte {
	buff := make([]byte, 1+4+8*len(ids))
	buff[0] = COMMAND_REMOVE_FROM_MAP

	binary.LittleEndian.PutUint32(buff[1:], uint32(len(ids)))
	for i, v := range ids {
		binary.LittleEndian.PutUint64(buff[(5+i*8):], v)
	}
	return buff
}

func DecodeRemoveFromMap(buff []byte) ([]uint64, error) {
	if buff == nil || len(buff) < 5 || buff[0] != COMMAND_REMOVE_FROM_MAP {
		return nil, Err_invalid_packet
	}
	length := binary.LittleEndian.Uint32(buff[1:])
	ids := make([]uint64, length)
	if length > 0 {
		for i := 0; i < int(length); i++ {
			ids[i] = binary.LittleEndian.Uint64(buff[(5 + i*8):])
		}
	}
	return ids, nil
}
