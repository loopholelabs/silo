package packets

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
)

func EncodeWriteAtCompGzip(offset int64, data []byte) []byte {
	var buff bytes.Buffer

	buff.WriteByte(CommandWriteAt)
	buff.WriteByte(WriteAtCompRLE)

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(offset))
	buff.Write(b)

	// Write the data compressed

	encoder, err := gzip.NewWriterLevel(&buff, gzip.BestSpeed)
	if err != nil {
		panic(err)
	}

	_, err = encoder.Write(data)
	if err != nil {
		panic(err)
	}

	err = encoder.Close()
	if err != nil {
		panic(err)
	}

	return buff.Bytes()
}

func DecodeWriteAtCompGzip(buff []byte) (offset int64, data []byte, err error) {
	if len(buff) < 10 || buff[0] != CommandWriteAt || buff[1] != WriteAtCompRLE {
		return 0, nil, ErrInvalidPacket
	}
	off := int64(binary.LittleEndian.Uint64(buff[2:]))

	decoder, err := gzip.NewReader(bytes.NewReader(buff[10:]))
	if err != nil {
		return 0, nil, err
	}

	d, err := io.ReadAll(decoder)

	return off, d, err
}
