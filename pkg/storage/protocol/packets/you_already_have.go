package packets

import (
	"bytes"
	"encoding/binary"
)

type YouAlreadyHave struct {
	BlockSize uint64
	Blocks    []uint32
}

func EncodeYouAlreadyHave(blockSize uint64, blocks []uint32) []byte {
	var buff bytes.Buffer

	head := make([]byte, 1+1+8+4)
	head[0] = CommandWriteAt
	head[1] = WriteAtYouAlreadyHave
	binary.LittleEndian.PutUint64(head[2:], blockSize)
	binary.LittleEndian.PutUint32(head[10:], uint32(len(blocks)))

	buff.Write(head)

	v := make([]byte, 4)
	for _, b := range blocks {
		binary.LittleEndian.PutUint32(v, b)
		buff.Write(v)
	}

	return buff.Bytes()
}

func DecodeYouAlreadyHave(buff []byte) (uint64, []uint32, error) {
	if buff == nil || len(buff) < 1+1+8+4 || buff[0] != CommandWriteAt || buff[1] != WriteAtYouAlreadyHave {
		return 0, nil, ErrInvalidPacket
	}

	blockSize := binary.LittleEndian.Uint64(buff[2:])
	l := int(binary.LittleEndian.Uint32(buff[10:]))

	blocks := make([]uint32, 0)
	ptr := 1 + 1 + 8 + 4
	for a := 0; a < l; a++ {
		b := binary.LittleEndian.Uint32(buff[ptr:])
		blocks = append(blocks, b)
		ptr += 4
	}

	return blockSize, blocks, nil
}
