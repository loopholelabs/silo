package protocol

import (
	"encoding/binary"
	"errors"
)

type DevInfo struct {
	Size uint64
}

func EncodeDevInfo(di *DevInfo) []byte {
	buff := make([]byte, 1+8)
	buff[0] = COMMAND_DEV_INFO
	binary.LittleEndian.PutUint64(buff[1:], di.Size)
	return buff
}

func DecodeDevInfo(buff []byte) (*DevInfo, error) {
	if buff == nil || len(buff) < 9 || buff[0] != COMMAND_DEV_INFO {
		return nil, errors.New("Invalid packet")
	}
	size := binary.LittleEndian.Uint64(buff[1:])
	return &DevInfo{Size: size}, nil
}
