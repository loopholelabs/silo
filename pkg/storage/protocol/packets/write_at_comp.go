package packets

import (
	"bytes"
	"encoding/binary"
	"errors"
)

func EncodeWriteAtComp(offset int64, data []byte) []byte {
	var buff bytes.Buffer

	buff.WriteByte(COMMAND_WRITE_AT_COMP)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(offset))
	buff.Write(b)

	// Now RLE the data
	MIN_LENGTH := 4
	areas := make(map[int]int, 0)

	ld := byte(0)
	length := 0
	for i, d := range data {
		if d != ld {
			// Flush the current run
			if length >= MIN_LENGTH {
				areas[i-length] = length
			}
			length = 1
			ld = d
		} else {
			length++ // Run length...
		}
	}

	if length > MIN_LENGTH {
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

	return buff.Bytes()
}

func DecodeWriteAtComp(buff []byte) (offset int64, data []byte, err error) {
	if buff == nil || len(buff) < 9 || buff[0] != COMMAND_WRITE_AT_COMP {
		return 0, nil, errors.New("Invalid packet command")
	}
	off := int64(binary.LittleEndian.Uint64(buff[1:]))

	var d bytes.Buffer

	// Now read the RLE data and put it in d buffer...
	p := 9
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
