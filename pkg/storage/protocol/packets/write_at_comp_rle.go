package packets

import (
	"bytes"
	"encoding/binary"
)

const rleMinLength = 4

func EncodeWriteAtCompRLE(offset int64, data []byte) ([]byte, error) {
	var buff bytes.Buffer

	buff.WriteByte(CommandWriteAt)
	buff.WriteByte(WriteAtCompRLE)

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(offset))
	buff.Write(b)

	// Now RLE the data
	areas := make(map[int]int, 0)

	ld := byte(0)
	length := 0
	for i, d := range data {
		if d != ld {
			// Flush the current run
			if length >= rleMinLength {
				areas[i-length] = length
			}
			length = 1
			ld = d
		} else {
			length++ // Run length...
		}
	}

	if length > rleMinLength {
		areas[len(data)-length] = length
	}

	// Now we encode the data...
	p := 0
	start := 0
	for {
		if p == len(data) {
			break
		}
		// Check if it's a RLE range
		l, ok := areas[p]
		if ok {
			// First flush any current range
			if start != p {

				dd := make([]byte, p-start+binary.MaxVarintLen32)
				lpos := binary.PutUvarint(dd, uint64(p-start)<<1)
				copy(dd[lpos:], data[start:p])
				buff.Write(dd[:lpos+(p-start)])
			}

			// TODO: Encode the data
			dd := make([]byte, binary.MaxVarintLen32+1)
			lpos := binary.PutUvarint(dd, (uint64(l)<<1)|1)
			dd[lpos] = data[p]
			buff.Write(dd[:lpos+1])

			p += l
			start = p
		} else {
			p++
		}
	}

	if start != p {
		dd := make([]byte, p-start+binary.MaxVarintLen32)
		lpos := binary.PutUvarint(dd, uint64(p-start)<<1)
		copy(dd[lpos:], data[start:p])
		buff.Write(dd[:lpos+(p-start)])
	}

	return buff.Bytes(), nil
}

func DecodeWriteAtCompRLE(buff []byte) (offset int64, data []byte, err error) {
	if len(buff) < 10 || buff[0] != CommandWriteAt || buff[1] != WriteAtCompRLE {
		return 0, nil, ErrInvalidPacket
	}
	off := int64(binary.LittleEndian.Uint64(buff[2:]))

	var d bytes.Buffer

	// Now read the RLE data and put it in d buffer...
	p := 10
	for {
		if p == len(buff) {
			break
		}
		// Read something
		l, llen := binary.Uvarint(buff[p:])
		p += llen
		if (l & 1) == 0 {
			// RAW
			d.Write(buff[p : p+(int(l)>>1)])
			p += (int(l) >> 1)
		} else {
			// RLE
			val := buff[p]
			// Write the value multiple times...
			for i := 0; i < int(l>>1); i++ {
				d.WriteByte(val)
			}
			p++
		}
	}

	return off, d.Bytes(), nil
}
