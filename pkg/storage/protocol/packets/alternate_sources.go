package packets

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

type AlternateSource struct {
	Offset   int64
	Length   int64
	Hash     [sha256.Size]byte
	Location string
}

func EncodeAlternateSources(sources []AlternateSource) []byte {
	var buff bytes.Buffer

	head := make([]byte, 1+4)
	head[0] = COMMAND_ALTERNATE_SOURCES
	binary.LittleEndian.PutUint32(head[1:], uint32(len(sources)))

	buff.Write(head)

	for _, src := range sources {
		v := make([]byte, 8+8+sha256.Size+4+len(src.Location))
		binary.LittleEndian.PutUint64(v, uint64(src.Offset))
		binary.LittleEndian.PutUint64(v[8:], uint64(src.Length))
		copy(v[16:], src.Hash[:])
		binary.LittleEndian.PutUint32(v[16+sha256.Size:], uint32(len(src.Location)))
		copy(v[16+sha256.Size+4:], src.Location)
		buff.Write(v)
	}

	return buff.Bytes()
}

func DecodeAlternateSources(buff []byte) ([]AlternateSource, error) {
	if buff == nil || len(buff) < 1+4 || buff[0] != COMMAND_ALTERNATE_SOURCES {
		return nil, ErrInvalidPacket
	}

	l := int(binary.LittleEndian.Uint32(buff[1:]))

	sources := make([]AlternateSource, 0)
	ptr := 5
	for a := 0; a < l; a++ {
		offset := binary.LittleEndian.Uint64(buff[ptr:])
		length := binary.LittleEndian.Uint64(buff[ptr+8:])
		hash := buff[ptr+16 : ptr+16+sha256.Size]
		locLen := binary.LittleEndian.Uint32(buff[ptr+16+sha256.Size:])
		loc := buff[ptr+16+sha256.Size+4 : ptr+16+sha256.Size+4+int(locLen)]
		ptr = ptr + 16 + sha256.Size + 4 + int(locLen)
		sources = append(sources, AlternateSource{
			Offset:   int64(offset),
			Length:   int64(length),
			Hash:     [32]byte(hash),
			Location: string(loc),
		})
	}

	return sources, nil
}
