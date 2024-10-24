package packets

import (
	"encoding/binary"
	"errors"
)

func EncodeDirtyList(block_size int, blocks []uint) []byte {
	buff := make([]byte, 1+4+4+4*len(blocks))
	buff[0] = COMMAND_DIRTY_LIST
	binary.LittleEndian.PutUint32(buff[1:], uint32(block_size))

	binary.LittleEndian.PutUint32(buff[5:], uint32(len(blocks)))
	for i, v := range blocks {
		binary.LittleEndian.PutUint32(buff[(9+i*4):], uint32(v))
	}
	return buff
}

func DecodeDirtyList(buff []byte) (int, []uint, error) {
	if buff == nil || len(buff) < 9 || buff[0] != COMMAND_DIRTY_LIST {
		return 0, nil, errors.New("Invalid packet")
	}
	block_size := binary.LittleEndian.Uint32(buff[1:])
	length := binary.LittleEndian.Uint32(buff[5:])
	blocks := make([]uint, length)
	if length > 0 {
		for i := 0; i < int(length); i++ {
			blocks[i] = uint(binary.LittleEndian.Uint32(buff[(9 + i*4):]))
		}
	}
	return int(block_size), blocks, nil
}

func EncodeDirtyListResponse() []byte {
	buff := make([]byte, 1)
	buff[0] = COMMAND_DIRTY_LIST_RESPONSE
	return buff
}

func DecodeDirtyListResponse(buff []byte) error {
	if buff == nil || len(buff) < 1 || buff[0] != COMMAND_DIRTY_LIST_RESPONSE {
		return errors.New("Invalid packet")
	}
	return nil
}
