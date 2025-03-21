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
	bfIf := NewBitfield(1000)

	bf.SetBit(99)

	bfIf.SetBit(99)
	bfIf.SetBit(100)

	bf.SetBitsIf(bfIf, 90, 105)

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

func TestBitfieldCollect(t *testing.T) {
	bf := NewBitfield(1000)

	bf.SetBits(100, 104)
	bf.SetBit(200)

	c := bf.Collect(0, bf.Length())

	assert.Equal(t, []uint{100, 101, 102, 103, 200}, c)
}

func TestBitfieldCollectZeroes(t *testing.T) {
	bf := NewBitfield(1000)

	bf.SetBits(0, 1000)
	bf.ClearBits(100, 104)
	bf.ClearBit(200)

	c := bf.CollectZeroes(0, bf.Length())

	assert.Equal(t, []uint{100, 101, 102, 103, 200}, c)
}

func TestBitfieldSetBitIfClear(t *testing.T) {
	bf := NewBitfield(1000)

	bf.SetBit(99)

	assert.Equal(t, true, bf.BitSet(99))
	assert.Equal(t, false, bf.BitSet(100))

	s99 := bf.SetBitIfClear(99)
	assert.True(t, s99)
	s100 := bf.SetBitIfClear(100)
	assert.False(t, s100)

	assert.Equal(t, true, bf.BitSet(99))
	assert.Equal(t, true, bf.BitSet(100))
}

func TestBitfieldShortText(t *testing.T) {
	bf := NewBitfield(1000)
	for _, v := range []int{7, 100, 101, 102, 905, 907} {
		bf.SetBit(v)
	}

	data := bf.GetShortText()
	assert.Equal(t, "7 100-102 905 907", data)

	nbf := NewBitfield(1000)
	nbf.LoadShortText(data)

	assert.True(t, bf.Equals(nbf))
}
