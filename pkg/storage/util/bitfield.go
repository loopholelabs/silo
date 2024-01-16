package util

import "sync/atomic"

type Bitfield struct {
	data []uint64
}

func NewBitfield(size int) *Bitfield {
	return &Bitfield{
		data: make([]uint64, (size+63)>>6),
	}
}

func (bf *Bitfield) Clear() {
	for i := 0; i < len(bf.data); i++ {
		bf.data[i] = 0
	}
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

func (bf *Bitfield) Length() uint {
	return uint(len(bf.data) >> 6)
}

// Create a mask of bits at the start of this range
func maskStart(start, end uint) (mask uint64) {
	const max = ^uint64(0)
	return ((max << (start & 63)) ^ (max << (end - start&^63))) & ((1 >> (start & 63)) - 1)
}

// Create a mask of bits at the end of this range
func maskEnd(start, end uint) (mask uint64) {
	const shiftBy = 31 + 32*(^uint(0)>>63)
	return ((1 << (end & 63)) - 1) & uint64((((end&^63-start)>>shiftBy)&1)-1)
}

/**
 * Set a number of bits in one op
 *
 */
func (bf *Bitfield) SetBits(start uint, end uint) {
	/*
		if start > end {
			panic("End less than start")
		}
		if end > bf.Length() {
			panic("Out of range")
		}
	*/

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
	/*
		if start > end {
			panic("End less than start")
		}
		if end > bf.Length() {
			panic("Out of range")
		}
	*/
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
	/*
		if start > end {
			panic("End less than start")
		}
		if end > bf.Length() {
			panic("Out of range")
		}
	*/

	p := start >> 6
	i := uint64(1 << (start & 63))
	n := start
	val := atomic.LoadUint64(&bf.data[p])
	for {
		if n == end {
			break
		}
		// Check the bit
		if (val & i) == 0 {
			return false
		}

		// Move along one...
		n++
		i = i << 1
		if i == 0 {
			i = 1
			p++
			val = atomic.LoadUint64(&bf.data[p])
		}
	}

	return true
}
