package packets

import (
	"bytes"
	"encoding/binary"
)

type DeviceGroupInfo struct {
	Devices map[int]*DevInfo
}

func EncodeDeviceGroupInfo(dgi *DeviceGroupInfo) []byte {
	var buffer bytes.Buffer
	buffer.WriteByte(CommandDeviceGroupInfo)
	diHeader := make([]byte, 8)

	for index, di := range dgi.Devices {
		diBytes := EncodeDevInfo(di)
		binary.LittleEndian.PutUint32(diHeader, uint32(index))
		binary.LittleEndian.PutUint32(diHeader[4:], uint32(len(diBytes)))
		buffer.Write(diHeader)
		buffer.Write(diBytes)
	}
	return buffer.Bytes()
}

func DecodeDeviceGroupInfo(buff []byte) (*DeviceGroupInfo, error) {
	dgi := &DeviceGroupInfo{
		Devices: make(map[int]*DevInfo),
	}

	if len(buff) < 1 || buff[0] != CommandDeviceGroupInfo {
		return nil, ErrInvalidPacket
	}

	ptr := 1
	for {
		if ptr == len(buff) {
			break
		}
		if len(buff)-ptr < 8 {
			return nil, ErrInvalidPacket
		}
		index := binary.LittleEndian.Uint32(buff[ptr:])
		length := binary.LittleEndian.Uint32(buff[ptr+4:])
		ptr += 8
		if len(buff)-ptr < int(length) {
			return nil, ErrInvalidPacket
		}
		di, err := DecodeDevInfo(buff[ptr : ptr+int(length)])
		if err != nil {
			return nil, err
		}
		dgi.Devices[int(index)] = di
		ptr += int(length)
	}
	return dgi, nil
}
