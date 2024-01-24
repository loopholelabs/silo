package blocks

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type AnyBlockOrder struct {
	lock       sync.Mutex
	num_blocks int
	available  util.Bitfield
	next       storage.BlockOrder
}

func NewAnyBlockOrder(num_blocks int, next storage.BlockOrder) *AnyBlockOrder {
	return &AnyBlockOrder{
		num_blocks: num_blocks,
		available:  *util.NewBitfield(num_blocks),
		next:       next,
	}
}

func (bo *AnyBlockOrder) Add(block int) {
	bo.lock.Lock()
	defer bo.lock.Unlock()
	bo.available.SetBit(block)
	if bo.next != nil {
		bo.next.Add(block)
	}
}

func (bo *AnyBlockOrder) Remove(block int) {
	bo.lock.Lock()
	defer bo.lock.Unlock()
	bo.available.ClearBit(block)
	if bo.next != nil {
		bo.next.Remove(block)
	}
}

// Get the next block...
func (bo *AnyBlockOrder) GetNext() *storage.BlockInfo {
	bo.lock.Lock()
	defer bo.lock.Unlock()

	// Find something available...
	for i := 0; i < bo.num_blocks; i++ {
		if bo.available.BitSet(i) {
			bo.available.ClearBit(i)
			if bo.next != nil {
				bo.next.Remove(i)
			}
			return &storage.BlockInfo{Block: i}
		}
	}

	if bo.next == nil {
		return storage.BlockInfoFinish
	}
	v := bo.next.GetNext()
	if v != storage.BlockInfoFinish {
		// Remove it from our own set
		bo.available.ClearBit(v.Block)
	}
	return v
}
