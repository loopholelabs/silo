package packets

import (
	"encoding/binary"
	"errors"
)

type DevInfo struct {
	Size       uint64
	Block_size uint32
	Name       string
	Schema     string
}

func EncodeDevInfo(di *DevInfo) []byte {
	buff := make([]byte, 1+8+4+2+len(di.Name)+4+len(di.Schema))
	buff[0] = COMMAND_DEV_INFO
	binary.LittleEndian.PutUint64(buff[1:], di.Size)
	binary.LittleEndian.PutUint32(buff[9:], di.Block_size)
	binary.LittleEndian.PutUint16(buff[13:], uint16(len(di.Name)))
	copy(buff[15:], []byte(di.Name))
	ptr := 15 + len(di.Name)
	binary.LittleEndian.PutUint32(buff[ptr:], uint32(len(di.Schema)))
	copy(buff[ptr+4:], []byte(di.Schema))
	return buff
}

func DecodeDevInfo(buff []byte) (*DevInfo, error) {
	if buff == nil || len(buff) < 19 || buff[0] != COMMAND_DEV_INFO {
		return nil, errors.New("Invalid packet")
	}
	size := binary.LittleEndian.Uint64(buff[1:])
	blocksize := binary.LittleEndian.Uint32(buff[9:])

	l := binary.LittleEndian.Uint16(buff[13:])
	if int(l)+15 > len(buff) {
		return nil, errors.New("Invalid packet")
	}
	name := string(buff[15 : 15+l])

	ptr := 15 + int(l)
	sl := binary.LittleEndian.Uint32(buff[ptr:])
	if ptr+4+int(sl) > len(buff) {
		return nil, errors.New("Invalid packet")
	}
	schema := string(buff[ptr+4 : ptr+4+int(sl)])

	return &DevInfo{Size: size, Block_size: blocksize, Name: name, Schema: schema}, nil
}
