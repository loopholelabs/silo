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
	switch CompressionImpl {
	case CompressRLE:
		return DecodeWriteAtCompRLE(buff)
	case CompressZeroes:
		return DecodeWriteAtCompZeroes(buff)
	case CompressGzip:
		return DecodeWriteAtCompGzip(buff)
	}
	return DecodeWriteAtCompRLE(buff)
}
