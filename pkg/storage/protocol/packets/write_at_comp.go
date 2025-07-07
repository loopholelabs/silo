package packets

var CompressionImpl = CompressRLE

const CompressRLE = 0
const CompressGzip = 1
const CompressZeroes = 2

func EncodeWriteAtComp(offset int64, data []byte) ([]byte, error) {
	switch CompressionImpl {
	case CompressRLE:
		return EncodeWriteAtCompRLE(offset, data)
	case CompressZeroes:
		return EncodeWriteAtCompZeroes(offset, data)
	case CompressGzip:
		return EncodeWriteAtCompGzip(offset, data)
	}
	return EncodeWriteAtCompRLE(offset, data)
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
	}
	return DecodeWriteAtCompRLE(buff)
}
