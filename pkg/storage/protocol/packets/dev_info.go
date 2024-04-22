package packets

import (
	"encoding/binary"
	"errors"
)

type DevInfo struct {
	Size      uint64
	BlockSize uint32
	Name      string
}

func EncodeDevInfo(di *DevInfo) []byte {
	buff := make([]byte, 1+8+4+2+len(di.Name))
	buff[0] = COMMAND_DEV_INFO
	binary.LittleEndian.PutUint64(buff[1:], di.Size)
	binary.LittleEndian.PutUint32(buff[9:], di.BlockSize)
	binary.LittleEndian.PutUint16(buff[13:], uint16(len(di.Name)))
	copy(buff[15:], []byte(di.Name))
	return buff
}

func DecodeDevInfo(buff []byte) (*DevInfo, error) {
	if buff == nil || len(buff) < 15 || buff[0] != COMMAND_DEV_INFO {
		return nil, errors.New("Invalid packet")
	}
	size := binary.LittleEndian.Uint64(buff[1:])
	blocksize := binary.LittleEndian.Uint32(buff[9:])

	l := binary.LittleEndian.Uint16(buff[13:])
	if int(l)+15 > len(buff) {
		return nil, errors.New("Invalid packet")
	}
	name := string(buff[15 : 15+l])
	return &DevInfo{Size: size, BlockSize: blocksize, Name: name}, nil
}
