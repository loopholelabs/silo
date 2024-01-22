package blocks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/**
 * Basic test, all come out, no dupes
 *
 */
func TestPriorityBlockOrder(t *testing.T) {
	abo_any := NewAnyBlockOrder(128, nil)
	abo := NewPriorityBlockOrder(128, abo_any)

	for i := 0; i < 128; i++ {
		abo.Add(i)
	}

	abo.PrioritiseBlock(57)

	done := make(map[int]bool)
	// Now pull them out and make sure they all come out, with no dupes

	// Make sure 57 comes out first...
	for i := 0; i < 128; i++ {
		ii := abo.GetNext()
		if i == 0 {
			assert.Equal(t, 57, ii)
		}
		_, ok := done[ii]
		assert.False(t, ok)
		done[ii] = true
	}

	// Should signal end
	assert.Equal(t, -1, abo.GetNext())
}
