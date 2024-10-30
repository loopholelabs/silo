package blocks

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type AnyBlockOrder struct {
	lock      sync.Mutex
	numBlocks int
	available util.Bitfield
	next      storage.BlockOrder
}

func NewAnyBlockOrder(numBlocks int, next storage.BlockOrder) *AnyBlockOrder {
	return &AnyBlockOrder{
		numBlocks: numBlocks,
		available: *util.NewBitfield(numBlocks),
		next:      next,
	}
}

func (bo *AnyBlockOrder) AddAll() {
	bo.lock.Lock()
	defer bo.lock.Unlock()
	bo.available.SetBits(0, uint(bo.numBlocks))
	if bo.next != nil {
		bo.next.AddAll()
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
	//	bo.lock.Lock()
	//	defer bo.lock.Unlock()

	// Find something available...
	n, err := bo.available.CollectFirstAndClear(0, bo.available.Length())
	if err == nil {
		if bo.next != nil {
			bo.next.Remove(int(n))
		}
		return &storage.BlockInfo{Block: int(n)}
	}
	/*
		for i := 0; i < bo.numBlocks; i++ {
			if bo.available.BitSet(i) {
				bo.available.ClearBit(i)
				if bo.next != nil {
					bo.next.Remove(i)
				}
				return &storage.BlockInfo{Block: i}
			}
		}
	*/
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
