package packets

import "fmt"

type CompressionType byte

const CompressionTypeRLE = WriteAtCompRLE
const CompressionTypeGzip = WriteAtCompGzip
const CompressionTypeZeroes = WriteAtCompZeroes

func EncodeWriteAtComp(compressionType CompressionType, offset int64, data []byte) ([]byte, error) {
	switch compressionType {
	case CompressionTypeRLE:
		return EncodeWriteAtCompRLE(offset, data)
	case CompressionTypeZeroes:
		return EncodeWriteAtCompZeroes(offset, data)
	case CompressionTypeGzip:
		return EncodeWriteAtCompGzip(offset, data)
	default:
		return nil, fmt.Errorf("unknown compression type %d", compressionType)
	}
}

func DecodeWriteAtComp(buff []byte) (offset int64, data []byte, err error) {
	if len(buff) < 2 || buff[0] != CommandWriteAt {
		return 0, nil, ErrInvalidPacket
	}
	compressionType := buff[1]
	switch compressionType {
	case WriteAtCompRLE:
		return DecodeWriteAtCompRLE(buff)
	case WriteAtCompZeroes:
		return DecodeWriteAtCompZeroes(buff)
	case WriteAtCompGzip:
		return DecodeWriteAtCompGzip(buff)
	default:
		return 0, nil, fmt.Errorf("unknown compression type %d", compressionType)
	}
}
