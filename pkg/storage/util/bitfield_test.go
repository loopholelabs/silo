package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitfieldSetBit(t *testing.T) {
	bf := NewBitfield(1000)

	bf.SetBit(99)

	assert.Equal(t, true, bf.BitSet(99))
	assert.Equal(t, false, bf.BitSet(100))

	bf.SetBit(99) // Check it doesn't do anything
	bf.SetBit(100)

	assert.Equal(t, true, bf.BitSet(99))
	assert.Equal(t, true, bf.BitSet(100))
}

func TestBitfieldClearBit(t *testing.T) {
	bf := NewBitfield(1000)

	bf.SetBit(99)

	assert.Equal(t, true, bf.BitSet(99))
	assert.Equal(t, false, bf.BitSet(100))

	bf.ClearBit(99)
	bf.ClearBit(100) // Check it doesn't do anything

	assert.Equal(t, false, bf.BitSet(99))
	assert.Equal(t, false, bf.BitSet(100))
}

func TestBitfieldSetBits(t *testing.T) {
	bf := NewBitfield(1000)
	// TODO: Add a few test cases here...
	bf.SetBits(100, 180)

	for i := 0; i < 100; i++ {
		assert.Equal(t, false, bf.BitSet(i))
	}

	for i := 100; i < 180; i++ {
		assert.Equal(t, true, bf.BitSet(i))
	}

	for i := 180; i < 1000; i++ {
		assert.Equal(t, false, bf.BitSet(i))
	}

}

func TestBitfieldClearBits(t *testing.T) {
	bf := NewBitfield(1000)
	bf.SetBits(0, 1000)

	// TODO: Add a few test cases here...
	bf.ClearBits(100, 180)

	for i := 0; i < 100; i++ {
		assert.Equal(t, true, bf.BitSet(i))
	}

	for i := 100; i < 180; i++ {
		assert.Equal(t, false, bf.BitSet(i))
	}

	for i := 180; i < 1000; i++ {
		assert.Equal(t, true, bf.BitSet(i))
	}

}

func TestBitfieldBitsSet(t *testing.T) {
	bf := NewBitfield(1000)
	// TODO: Add a few test cases here...
	bf.SetBits(100, 180)

	assert.Equal(t, true, bf.BitsSet(100, 180))
	assert.Equal(t, false, bf.BitsSet(100, 185))
	assert.Equal(t, false, bf.BitsSet(0, 1000))

	// Make a gap
	bf.ClearBit(150)
	assert.Equal(t, false, bf.BitsSet(100, 180))

}

func TestBitfieldClear(t *testing.T) {
	bf := NewBitfield(1000)
	// TODO: Add a few test cases here...
	bf.SetBits(100, 180)

	bf.Clear()

	for i := 0; i < 1000; i++ {
		assert.Equal(t, false, bf.BitSet(i))
	}
}

func TestBitfieldSetBitsIf(t *testing.T) {
	bf := NewBitfield(1000)
	bf_if := NewBitfield(1000)

	bf.SetBit(99)

	bf_if.SetBit(99)
	bf_if.SetBit(100)

	bf.SetBitsIf(bf_if, 90, 105)

	for i := 0; i < 1000; i++ {
		if i == 99 || i == 100 {
			assert.Equal(t, true, bf.BitSet(i))

		} else {
			assert.Equal(t, false, bf.BitSet(i))

		}
	}
}

func TestBitfieldCount(t *testing.T) {
	bf := NewBitfield(1000)

	assert.Equal(t, 0, bf.Count(0, 1000))

	bf.SetBits(100, 180)

	assert.Equal(t, 80, bf.Count(0, 1000))

	bf.SetBits(100, 180)

	assert.Equal(t, 5, bf.Count(95, 105))
}

func TestBitfieldEmpty(t *testing.T) {
	bf := NewBitfield(1000)

	assert.True(t, bf.Empty())

	bf.SetBits(100, 180)

	assert.False(t, bf.Empty())

	bf.ClearBits(100, 180)

	assert.True(t, bf.Empty())

}