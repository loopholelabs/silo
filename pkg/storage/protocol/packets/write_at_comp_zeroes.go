package packets

import (
	"bytes"
	"encoding/binary"
)

const TargetNumZeroes = 12

// 1 byte CommandWriteAt
// 1 byte WriteAtCompRLE
// 4 byte offset
// 4 byte length
// {
// 		varint startRange
// 		varint lengthRange
// 		range data
// }

func EncodeWriteAtCompZeroes(offset int64, data []byte) ([]byte, error) {
	var buff bytes.Buffer

	vbuff := make([]byte, binary.MaxVarintLen32+8)

	buff.WriteByte(CommandWriteAt)
	buff.WriteByte(WriteAtCompZeroes)

	binary.LittleEndian.PutUint64(vbuff, uint64(offset))
	buff.Write(vbuff[:8])
	binary.LittleEndian.PutUint64(vbuff, uint64(len(data)))
	buff.Write(vbuff[:8])

	p := 0 // Current position
mainloop:
	for {
		// Find the next non-zero byte
		for {
			if p == len(data) { // There's no more data. We're all done now.
				break mainloop
			}
			if data[p] != 0 {
				break
			}
			p++
		}

		startRange := p

		// Find a group of zeroes in the future
	findend:
		for {
			if p == len(data) { // All done.
				break findend
			}
			if data[p] == 0 {
				goodStretch := true
				// Make sure it's a good stretch of zeroes
				for t := 0; t < TargetNumZeroes; t++ {
					if p+t == len(data) {
						break // All done
					}
					if data[p+t] != 0 {
						goodStretch = false
						break
					}
				}
				if goodStretch {
					break findend
				}
			}
			p++
		}

		// Now encode it (startRange - p)
		lpos := binary.PutVarint(vbuff, int64(startRange))
		buff.Write(vbuff[:lpos])
		lpos = binary.PutVarint(vbuff, int64(p-startRange))
		buff.Write(vbuff[:lpos])
		buff.Write(data[startRange:p])
	}

	return buff.Bytes(), nil
}

func DecodeWriteAtCompZeroes(buff []byte) (offset int64, data []byte, err error) {
	if len(buff) < 18 || buff[0] != CommandWriteAt || buff[1] != WriteAtCompZeroes {
		return 0, nil, ErrInvalidPacket
	}
	off := int64(binary.LittleEndian.Uint64(buff[2:]))
	length := int64(binary.LittleEndian.Uint64(buff[10:]))

	d := make([]byte, length)

	p := 18
	for {
		if p == len(buff) {
			break
		}
		rangeStart, n := binary.Varint(buff[p:])
		p += n
		rangeLength, n := binary.Varint(buff[p:])
		p += n
		n = copy(d[rangeStart:], buff[p:p+int(rangeLength)])
		p += n
	}

	return off, d, nil
}
