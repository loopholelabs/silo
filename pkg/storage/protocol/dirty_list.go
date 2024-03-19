package protocol

import (
	"encoding/binary"
	"errors"
)

func EncodeDirtyList(blocks []uint) []byte {
	buff := make([]byte, 1+4+4*len(blocks))
	buff[0] = COMMAND_DIRTY_LIST
	binary.LittleEndian.PutUint32(buff[1:], uint32(len(blocks)))
	for i, v := range blocks {
		binary.LittleEndian.PutUint32(buff[(5+i*4):], uint32(v))
	}
	return buff
}

func DecodeDirtyList(buff []byte) ([]uint, error) {
	if buff == nil || len(buff) < 5 || buff[0] != COMMAND_DIRTY_LIST {
		return nil, errors.New("Invalid packet")
	}
	length := binary.LittleEndian.Uint32(buff[1:])
	blocks := make([]uint, length)
	if length > 0 {
		for i := 0; i < int(length); i++ {
			blocks[i] = uint(binary.LittleEndian.Uint32(buff[(5 + i*4):]))
		}
	}
	return blocks, nil
}
