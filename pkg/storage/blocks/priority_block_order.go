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

type PriorityBlockInfo struct {
	storage.BlockInfo
	Time time.Time
}

func NewPriorityBlockOrder(num_blocks int, next storage.BlockOrder) *PriorityBlockOrder {
	return &PriorityBlockOrder{
		num_blocks:      num_blocks,
		available:       *util.NewBitfield(num_blocks),
		next:            next,
		priority_blocks: make(map[uint]time.Time),
	}
}

func (bo *PriorityBlockOrder) AddAll() {
	bo.lock.Lock()
	defer bo.lock.Unlock()
	bo.available.SetBits(0, uint(bo.num_blocks))
	if bo.next != nil {
		bo.next.AddAll()
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
func (bo *PriorityBlockOrder) GetNext() *storage.BlockInfo {
	bo.lock.Lock()
	// If we have any priority blocks, return them in order
	//
	earliest := time.Now()
	earliestBlock := -1
	for b, t := range bo.priority_blocks {
		if bo.available.BitSet(int(b)) {
			if t.Before(earliest) {
				earliest = t
				earliestBlock = int(b)
			}
		}
	}

	// If we found something, remove, and return it...
	if earliestBlock != -1 {
		delete(bo.priority_blocks, uint(earliestBlock))
		bo.available.ClearBit(earliestBlock)
		// Remove it downstream as well.
		if bo.next != nil {
			bo.next.Remove(earliestBlock)
		}
		bo.lock.Unlock()
		return &storage.BlockInfo{Block: earliestBlock, Type: storage.BlockTypePriority}
	}
	bo.lock.Unlock()

	if bo.next == nil {
		return storage.BlockInfoFinish
	}
	v := bo.next.GetNext()
	if v != storage.BlockInfoFinish {
		// Remove it from our own set
		bo.lock.Lock()
		delete(bo.priority_blocks, uint(v.Block))
		bo.available.ClearBit(v.Block)
		bo.lock.Unlock()
	}
	return v
}
