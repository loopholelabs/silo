package packets

import (
	"crypto/sha256"
	"encoding/binary"
)

func EncodeHashes(hashes map[uint][sha256.Size]byte) []byte {

	buff := make([]byte, 1+((4+sha256.Size)*len(hashes)))
	buff[0] = CommandHashes
	ptr := 1
	for i, v := range hashes {
		binary.LittleEndian.PutUint32(buff[ptr:], uint32(i))
		ptr += 4
		copy(buff[ptr:], v[:])
		ptr += sha256.Size
	}
	return buff
}

func DecodeHashes(buff []byte) (map[uint][sha256.Size]byte, error) {
	if len(buff) < 1 || buff[0] != CommandHashes {
		return nil, ErrInvalidPacket
	}
	hashes := make(map[uint][sha256.Size]byte)
	ptr := 1
	for {
		if ptr == len(buff) {
			break
		}
		if ptr+(4+sha256.Size) > len(buff) {
			return nil, ErrInvalidPacket
		}
		b := binary.LittleEndian.Uint32(buff[ptr:])
		ptr += 4
		v := buff[ptr : ptr+sha256.Size]
		ptr += sha256.Size
		hashes[uint(b)] = [sha256.Size]byte(v)
	}
	return hashes, nil
}

func EncodeHashesResponse() []byte {
	return []byte{CommandHashesResponse}
}

func DecodeHashesResponse(buff []byte) error {
	if buff == nil || len(buff) != 1 || buff[0] != CommandHashesResponse {
		return ErrInvalidPacket
	}
	return nil
}
