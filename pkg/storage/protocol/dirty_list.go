package protocol

import (
	"encoding/binary"
	"errors"
)

func EncodeDirtyList(blocks []uint32) []byte {
	buff := make([]byte, 1+4+4*len(blocks))
	buff[0] = COMMAND_DIRTY_LIST
	binary.LittleEndian.PutUint32(buff[1:], uint32(len(blocks)))
	for i, v := range blocks {
		binary.LittleEndian.PutUint32(buff[(5+i*4):], v)
	}
	return buff
}

func DecodeDirtyList(buff []byte) ([]uint32, error) {
	if buff == nil || len(buff) < 9 || buff[0] != COMMAND_DIRTY_LIST {
		return nil, errors.New("Invalid packet")
	}
	length := binary.LittleEndian.Uint32(buff[1:])
	blocks := make([]uint32, length)
	for i := 0; i < int(length); i++ {
		blocks[i] = binary.LittleEndian.Uint32(buff[(5 + i*4):])
	}
	return blocks, nil
}
