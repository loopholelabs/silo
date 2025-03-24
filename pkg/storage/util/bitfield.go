package util

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

type Bitfield struct {
	data []uint64
	size int
}

/**
 * Create a new bitfield with the specified size
 *
 */
func NewBitfield(size int) *Bitfield {
	return &Bitfield{
		size: size,
		data: make([]uint64, (size+63)>>6),
	}
}

/**
 * Clone this into a new separate Bitfield
 *
 */
func (bf *Bitfield) Clone() *Bitfield {
	data2 := make([]uint64, len(bf.data))
	for i := 0; i < len(bf.data); i++ {
		v := atomic.LoadUint64(&bf.data[i])
		data2[i] = v
	}
	// copy(data2, bf.data)		// Can't use, not atomic
	return &Bitfield{
		size: bf.size,
		data: data2,
	}
}

/**
 * Clear the bitfield
 *
 */
func (bf *Bitfield) Clear() {
	for i := 0; i < len(bf.data); i++ {
		atomic.StoreUint64(&bf.data[i], 0)
	}
}

/**
 * Check if empty
 *
 */
func (bf *Bitfield) Empty() bool {
	for i := 0; i < len(bf.data); i++ {
		v := atomic.LoadUint64(&bf.data[i])
		if v != 0 {
			return false
		}
	}
	return true
}

/**
 * Set the specified bit
 *
 */
func (bf *Bitfield) SetBit(i int) {
	f := uint64(1 << (i & 63))
	p := i >> 6
	old := atomic.LoadUint64(&bf.data[p])
	for !atomic.CompareAndSwapUint64(&bf.data[p], old, old|f) {
		old = atomic.LoadUint64(&bf.data[p])
	}
}

/**
 * Clear the specified bit
 *
 */
func (bf *Bitfield) ClearBit(i int) {
	f := uint64(1 << (i & 63))
	p := i >> 6
	old := atomic.LoadUint64(&bf.data[p])
	for !atomic.CompareAndSwapUint64(&bf.data[p], old, old&^f) {
		old = atomic.LoadUint64(&bf.data[p])
	}
}

/**
 * Check if the specified bit is currently set
 *
 */
func (bf *Bitfield) BitSet(i int) bool {
	f := uint64(1 << (i & 63))
	p := i >> 6
	old := atomic.LoadUint64(&bf.data[p])
	return (old & f) != 0
}

/**
 * Set the bit. Returns the previous state of the bit
 *
 */
func (bf *Bitfield) SetBitIfClear(i int) bool {
	f := uint64(1 << (i & 63))
	p := i >> 6
	old := atomic.LoadUint64(&bf.data[p])
	for !atomic.CompareAndSwapUint64(&bf.data[p], old, old|f) {
		old = atomic.LoadUint64(&bf.data[p])
	}
	return (old & f) != 0
}

/**
 * Get the length of the bitfield
 *
 */
func (bf *Bitfield) Length() uint {
	return uint(bf.size)
}

/**
 * Set a number of bits in one op
 *
 */
func (bf *Bitfield) SetBits(start uint, end uint) {
	if mask := maskStart(start, end); mask != 0 {
		p := start >> 6
		old := atomic.LoadUint64(&bf.data[p])
		for !atomic.CompareAndSwapUint64(&bf.data[p], old, old|mask) {
			old = atomic.LoadUint64(&bf.data[p])
		}
	}

	// Fill in any middle section
	for i := (start + 63) &^ 63; i < end&^63; i += 64 {
		p := i >> 6
		atomic.StoreUint64(&bf.data[p], ^uint64(0))
	}

	if mask := maskEnd(start, end); mask != 0 {
		p := end >> 6
		old := atomic.LoadUint64(&bf.data[p])
		for !atomic.CompareAndSwapUint64(&bf.data[p], old, old|mask) {
			old = atomic.LoadUint64(&bf.data[p])
		}
	}
}

/**
 * Clear a number of bits in one op
 *
 */
func (bf *Bitfield) ClearBits(start uint, end uint) {
	if mask := maskStart(start, end); mask != 0 {
		p := start >> 6
		old := atomic.LoadUint64(&bf.data[p])
		for !atomic.CompareAndSwapUint64(&bf.data[p], old, old&^mask) {
			old = atomic.LoadUint64(&bf.data[p])
		}
	}

	// Fill in any middle section
	for i := (start + 63) &^ 63; i < end&^63; i += 64 {
		p := i >> 6
		atomic.StoreUint64(&bf.data[p], 0)
	}

	if mask := maskEnd(start, end); mask != 0 {
		p := end >> 6
		old := atomic.LoadUint64(&bf.data[p])
		for !atomic.CompareAndSwapUint64(&bf.data[p], old, old&^mask) {
			old = atomic.LoadUint64(&bf.data[p])
		}
	}

}

/**
 * Check if a range of bits are all set
 * NB This is NOT atomic with SetBits/ClearBits. So there may be a set/clear that is partially completed at this check.
 */
func (bf *Bitfield) BitsSet(start uint, end uint) bool {
	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	if n < end {
		val := atomic.LoadUint64(&bf.data[p])
		for {
			// Check the bit
			if (val & i) == 0 {
				return false
			}

			// Move along one...
			n++
			if n == end {
				break
			}
			i <<= 1
			if i == 0 {
				i = 1
				p++
				val = atomic.LoadUint64(&bf.data[p])
			}
		}
	}
	return true
}

/**
 * Set bits within the range IF they are also set in if_bf
 * NB This is NOT atomic with SetBits/ClearBits. So there may be a set/clear that is partially completed at this check.
 */
func (bf *Bitfield) SetBitsIf(ifBf *Bitfield, start uint, end uint) {
	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	if n < end {
		val := atomic.LoadUint64(&ifBf.data[p])
		for {
			// Check the bit
			if (val & i) != 0 {
				// Set the bit in bf
				old := atomic.LoadUint64(&bf.data[p])
				for !atomic.CompareAndSwapUint64(&bf.data[p], old, old|i) {
					old = atomic.LoadUint64(&bf.data[p])
				}
			}

			// Move along one...
			n++
			if n == end {
				break
			}
			i <<= 1
			if i == 0 {
				i = 1
				p++
				val = atomic.LoadUint64(&ifBf.data[p])
			}
		}
	}
}

/**
 * Clear bits within the range IF they are set in if_bf
 * NB This is NOT atomic with SetBits/ClearBits. So there may be a set/clear that is partially completed at this check.
 */
func (bf *Bitfield) ClearBitsIf(ifBf *Bitfield, start uint, end uint) {
	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	if n < end {
		val := atomic.LoadUint64(&ifBf.data[p])
		for {
			// Check the bit
			if (val & i) != 0 {
				// Set the bit in bf
				old := atomic.LoadUint64(&bf.data[p])
				for !atomic.CompareAndSwapUint64(&bf.data[p], old, old&^i) {
					old = atomic.LoadUint64(&bf.data[p])
				}
			}

			// Move along one...
			n++
			if n == end {
				break
			}
			i <<= 1
			if i == 0 {
				i = 1
				p++
				val = atomic.LoadUint64(&ifBf.data[p])
			}
		}
	}
}

/**
 * Count bits
 */
func (bf *Bitfield) Count(start uint, end uint) int {
	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	count := 0
	if n < end {
		val := atomic.LoadUint64(&bf.data[p])
		for {
			// Check the bit
			if (val & i) != 0 {
				count++
			}

			// Move along one...
			n++
			if n == end {
				break
			}
			i <<= 1
			if i == 0 {
				i = 1
				p++
				val = atomic.LoadUint64(&bf.data[p])
			}
		}
	}
	return count
}

/**
 * Execute something for 1 bits.
 * The function can clear the bit if it returns false.
 */
func (bf *Bitfield) Exec(start uint, end uint, cb func(position uint) bool) {
	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	if n < end {
		val := atomic.LoadUint64(&bf.data[p])
		for {
			// Check the bit
			if (val & i) != 0 {
				if !cb(n) {
					// Clear the bit
					old := atomic.LoadUint64(&bf.data[p])
					for !atomic.CompareAndSwapUint64(&bf.data[p], old, old&^i) {
						old = atomic.LoadUint64(&bf.data[p])
					}
				}
			}

			// Move along one...
			n++
			if n == end {
				break
			}
			i <<= 1
			if i == 0 {
				i = 1
				p++
				val = atomic.LoadUint64(&bf.data[p])
			}
		}
	}
}

/**
 * Collect the positions of all 1 bits
 *
 */
func (bf *Bitfield) Collect(start uint, end uint) []uint {
	positions := make([]uint, 0)
	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	if n < end {
		val := atomic.LoadUint64(&bf.data[p])
		for {
			// Check the bit
			if (val & i) != 0 {
				positions = append(positions, n)
			}

			// Move along one...
			n++
			if n == end {
				break
			}
			i <<= 1
			if i == 0 {
				i = 1
				p++
				val = atomic.LoadUint64(&bf.data[p])
			}
		}
	}
	return positions
}

/**
 * Collect the positions of all 0 bits
 *
 */
func (bf *Bitfield) CollectZeroes(start uint, end uint) []uint {
	positions := make([]uint, 0)
	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	if n < end {
		val := atomic.LoadUint64(&bf.data[p])
		for {
			// Check the bit
			if (val & i) == 0 {
				positions = append(positions, n)
			}

			// Move along one...
			n++
			if n == end {
				break
			}
			i <<= 1
			if i == 0 {
				i = 1
				p++
				val = atomic.LoadUint64(&bf.data[p])
			}
		}
	}
	return positions
}

/**
 * Collect the first set bit, and clear it
 *
 */
func (bf *Bitfield) CollectFirstAndClear(start uint, end uint) (uint, error) {
	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	if n < end {
		val := atomic.LoadUint64(&bf.data[p])
		for {
			// Check the bit
			if (val & i) != 0 {
				// Clear it
				for !atomic.CompareAndSwapUint64(&bf.data[p], val, val&^i) {
					val = atomic.LoadUint64(&bf.data[p])
				}
				return n, nil
			}

			// Move along one...
			n++
			if n == end {
				break
			}
			i <<= 1
			if i == 0 {
				i = 1
				p++
				val = atomic.LoadUint64(&bf.data[p])
			}
		}
	}
	return 0, errors.New("Nothing left")
}

/**
 * Check for equals
 *
 */
func (bf *Bitfield) Equals(bf2 *Bitfield) bool {
	if len(bf.data) != len(bf2.data) {
		return false
	}

	l := len(bf.data)
	for i := 0; i < l; i++ {
		v1 := atomic.LoadUint64(&bf.data[i])
		v2 := atomic.LoadUint64(&bf2.data[i])
		if v1 != v2 {
			return false
		}
	}
	return true
}

func (bf *Bitfield) GetShortText() string {
	data := ""
	bits := bf.Collect(0, bf.Length())
	currentRangeStart := -1
	lastv := -1
	for _, v := range bits {
		if currentRangeStart != -1 {
			if int(v) != lastv+1 {
				// Close the current range and start again...
				if currentRangeStart == lastv {
					data = fmt.Sprintf("%s %d", data, currentRangeStart)
				} else {
					data = fmt.Sprintf("%s %d-%d", data, currentRangeStart, lastv)
				}
				currentRangeStart = int(v)
			}
		} else {
			currentRangeStart = int(v)
		}

		lastv = int(v)
	}

	// Close any remaining block
	if currentRangeStart != -1 {
		if currentRangeStart == lastv {
			data = fmt.Sprintf("%s %d", data, currentRangeStart)
		} else {
			data = fmt.Sprintf("%s %d-%d", data, currentRangeStart, lastv)
		}
	}
	return strings.Trim(data, " ")
}

func (bf *Bitfield) LoadShortText(data string) error {
	// Load up the bits from a ShortText representation
	bits := strings.Split(data, " ")
	for _, b := range bits {
		vals := strings.Split(b, "-")
		switch len(vals) {
		case 1:
			val1, err := strconv.ParseInt(vals[0], 10, 64)
			if err != nil {
				return err
			}
			bf.SetBit(int(val1))
		case 2:
			val1, err := strconv.ParseInt(vals[0], 10, 64)
			if err != nil {
				return err
			}
			val2, err := strconv.ParseInt(vals[1], 10, 64)
			if err != nil {
				return err
			}
			if val2 <= val1 {
				return errors.New("malformed data")
			}
			bf.SetBits(uint(val1), uint(val2)+1)
		default:
			return errors.New("malformed data")
		}
	}
	return nil
}

// Create a mask of bits at the start of this range
func maskStart(start, end uint) (mask uint64) {
	const maxUint64 = ^uint64(0)
	return ((maxUint64 << (start & 63)) ^ (maxUint64 << (end - start&^63))) & ((1 >> (start & 63)) - 1)
}

// Create a mask of bits at the end of this range
func maskEnd(start, end uint) (mask uint64) {
	const shiftBy = 31 + 32*(^uint(0)>>63)
	return ((1 << (end & 63)) - 1) & uint64((((end&^63-start)>>shiftBy)&1)-1)
}
