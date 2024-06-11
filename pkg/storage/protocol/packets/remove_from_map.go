package packets

import (
	"encoding/binary"
	"errors"
)

func EncodeRemoveFromMap(ids []uint) []byte {
	buff := make([]byte, 1+4+4*len(ids))
	buff[0] = COMMAND_REMOVE_FROM_MAP

	binary.LittleEndian.PutUint32(buff[1:], uint32(len(ids)))
	for i, v := range ids {
		binary.LittleEndian.PutUint32(buff[(5+i*4):], uint32(v))
	}
	return buff
}

func DecodeRemoveFromMap(buff []byte) ([]uint, error) {
	if buff == nil || len(buff) < 5 || buff[0] != COMMAND_REMOVE_FROM_MAP {
		return nil, errors.New("Invalid packet")
	}
	length := binary.LittleEndian.Uint32(buff[1:])
	ids := make([]uint, length)
	if length > 0 {
		for i := 0; i < int(length); i++ {
			ids[i] = uint(binary.LittleEndian.Uint32(buff[(5 + i*4):]))
		}
	}
	return ids, nil
}
