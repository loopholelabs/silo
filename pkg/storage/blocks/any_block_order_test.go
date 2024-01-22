package blocks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/**
 * Basic test, all come out, no dupes
 *
 */
func TestAnyBlockOrder(t *testing.T) {
	abo := NewAnyBlockOrder(128, nil)

	for i := 0; i < 128; i++ {
		abo.Add(i)
	}

	done := make(map[int]bool)
	// Now pull them out and make sure they all come out, with no dupes

	for i := 0; i < 128; i++ {
		ii := abo.GetNext()
		_, ok := done[ii]
		assert.False(t, ok)
		done[ii] = true
	}

	// Should signal end
	assert.Equal(t, -1, abo.GetNext())
}

/**
 * Test next(), all come out, no dupes
 *
 */
func TestAnyBlockOrderNext(t *testing.T) {
	abo := NewAnyBlockOrder(128, nil)

	abo2 := NewAnyBlockOrder(128, abo)

	for i := 0; i < 128; i++ {
		if i > 100 {
			abo2.Add(i)
		}
		abo.Add(i)
	}

	done := make(map[int]bool)
	// Now pull them out and make sure they all come out, with no dupes

	for i := 0; i < 128; i++ {
		ii := abo2.GetNext()
		_, ok := done[ii]
		assert.False(t, ok)
		done[ii] = true
	}

	// Should signal end
	assert.Equal(t, -1, abo.GetNext())
	// Should signal end
	assert.Equal(t, -1, abo2.GetNext())
}

func TestAnyBlockOrderRemove(t *testing.T) {
	abo := NewAnyBlockOrder(128, nil)

	for i := 0; i < 128; i++ {
		abo.Add(i)
	}

	// Remove one
	abo.Remove(100)

	done := make(map[int]bool)
	// Now pull them out and make sure they all come out, with no dupes

	for i := 0; i < 127; i++ {
		ii := abo.GetNext()
		_, ok := done[ii]
		assert.False(t, ok)
		done[ii] = true
	}

	_, ok_100 := done[100]
	assert.False(t, ok_100)

	// Should signal end
	assert.Equal(t, -1, abo.GetNext())
}
