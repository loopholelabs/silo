package packets

func EncodeWriteAtComp(compressionType byte, offset int64, data []byte) ([]byte, error) {
	switch compressionType {
	case WriteAtCompRLE:
		return EncodeWriteAtCompRLE(offset, data)
	case WriteAtCompZeroes:
		return EncodeWriteAtCompZeroes(offset, data)
	case WriteAtCompGzip:
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
