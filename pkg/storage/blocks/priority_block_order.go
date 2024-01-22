package blocks

import (
	"sync"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type PriorityBlockOrder struct {
	lock            sync.Mutex
	priority_blocks map[uint]time.Time
	num_blocks      int
	available       util.Bitfield
	next            storage.BlockOrder
}

func NewPriorityBlockOrder(num_blocks int, next storage.BlockOrder) *PriorityBlockOrder {
	return &PriorityBlockOrder{
		num_blocks:      num_blocks,
		available:       *util.NewBitfield(num_blocks),
		next:            next,
		priority_blocks: make(map[uint]time.Time),
	}
}

func (bo *PriorityBlockOrder) Add(block int) {
	bo.lock.Lock()
	defer bo.lock.Unlock()
	bo.available.SetBit(block)
	if bo.next != nil {
		bo.next.Add(block)
	}
}

func (bo *PriorityBlockOrder) Remove(block int) {
	bo.lock.Lock()
	defer bo.lock.Unlock()
	bo.available.ClearBit(block)
	if bo.next != nil {
		bo.next.Remove(block)
	}
}

func (bo *PriorityBlockOrder) PrioritiseBlock(block int) bool {
	bo.lock.Lock()
	defer bo.lock.Unlock()

	if bo.available.BitSet(block) {
		// Update
		_, ok := bo.priority_blocks[uint(block)]
		if !ok {
			bo.priority_blocks[uint(block)] = time.Now()
		}
		// If we already have it as a priority, ignore the request
		return true
	}
	return false
}

// Get the next block...
func (bo *PriorityBlockOrder) GetNext() int {
	bo.lock.Lock()
	defer bo.lock.Unlock()
	// If we have any priority blocks, return them in order
	//
	earliest := time.Now()
	earliest_block := -1
	for b, t := range bo.priority_blocks {
		if bo.available.BitSet(int(b)) {
			if t.Before(earliest) {
				earliest = t
				earliest_block = int(b)
			}
		}
	}

	// If we found something, remove, and return it...
	if earliest_block != -1 {
		delete(bo.priority_blocks, uint(earliest_block))
		bo.available.ClearBit(earliest_block)
		// Remove it downstream as well.
		if bo.next != nil {
			bo.next.Remove(earliest_block)
		}
		return earliest_block
	}

	if bo.next == nil {
		return -1
	}
	v := bo.next.GetNext()
	if v != -1 {
		// Remove it from our own set
		delete(bo.priority_blocks, uint(v))
		bo.available.ClearBit(v)
	}
	return v
}
