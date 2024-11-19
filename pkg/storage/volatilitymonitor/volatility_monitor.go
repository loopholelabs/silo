package volatilitymonitor

import (
	"sync"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type VolatilityMonitor struct {
	storage.ProviderWithEvents
	prov          storage.Provider
	expiry        time.Duration
	size          uint64
	numBlocks     int
	blockSize     int
	blockDataLock sync.Mutex
	available     util.Bitfield
	blockData     map[uint]*volatilityData
	totalData     *volatilityData
}

type Metrics struct {
	BlockSize  uint64
	Available  uint64
	Volatility uint64
}

// Relay events to embedded StorageProvider
func (i *VolatilityMonitor) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewVolatilityMonitor(prov storage.Provider, blockSize int, expiry time.Duration) *VolatilityMonitor {
	numBlocks := (int(prov.Size()) + blockSize - 1) / blockSize
	return &VolatilityMonitor{
		prov:      prov,
		size:      prov.Size(),
		numBlocks: numBlocks,
		blockSize: blockSize,
		blockData: make(map[uint]*volatilityData),
		available: *util.NewBitfield(numBlocks),
		expiry:    expiry,
		totalData: &volatilityData{log: make([]int64, 0)},
	}
}

func (i *VolatilityMonitor) GetMetrics() *Metrics {
	return &Metrics{
		BlockSize:  uint64(i.blockSize),
		Available:  uint64(i.available.Count(0, i.available.Length())),
		Volatility: uint64(i.GetTotalVolatility()),
	}
}

// from storage.BlockOrder

func (i *VolatilityMonitor) GetNext() *storage.BlockInfo {
	block := -1 // All done
	blockCount := 0

	// Find something to return...
	i.blockDataLock.Lock()
	defer i.blockDataLock.Unlock()

	for n := 0; n < i.numBlocks; n++ {
		if i.available.BitSet(n) {
			bd, ok := i.blockData[uint(n)]
			c := 0
			if ok {
				c = bd.Count(i.expiry)
			}

			if block == -1 || (c <= blockCount) {
				block = n
				blockCount = c
				if blockCount == 0 {
					break // Special case - this is a static block. Not going to find better.
				}
			}
		}
	}

	if block != -1 {
		delete(i.blockData, uint(block))
		i.available.ClearBit(block)
		return &storage.BlockInfo{Block: block}
	}
	return storage.BlockInfoFinish
}

func (i *VolatilityMonitor) AddAll() {
	i.available.SetBits(0, uint(i.numBlocks))
}

func (i *VolatilityMonitor) Add(block int) {
	i.available.SetBit(block)
}

func (i *VolatilityMonitor) Remove(block int) {
	i.blockDataLock.Lock()
	delete(i.blockData, uint(block))
	i.blockDataLock.Unlock()
	i.available.ClearBit(block)
}

func (i *VolatilityMonitor) GetVolatility(block int) int {
	i.blockDataLock.Lock()
	defer i.blockDataLock.Unlock()
	bd, ok := i.blockData[uint(block)]
	if ok {
		return bd.Count(i.expiry)
	}
	return 0
}

func (i *VolatilityMonitor) GetTotalVolatility() int {
	i.blockDataLock.Lock()
	defer i.blockDataLock.Unlock()
	return i.totalData.Count(i.expiry)
}

// From storage.StorageProvider

func (i *VolatilityMonitor) ReadAt(buffer []byte, offset int64) (int, error) {
	return i.prov.ReadAt(buffer, offset)
}

func (i *VolatilityMonitor) WriteAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	bStart := uint(offset / int64(i.blockSize))
	bEnd := uint((end-1)/uint64(i.blockSize)) + 1

	n, err := i.prov.WriteAt(buffer, offset)

	if err == nil {
		for block := bStart; block < bEnd; block++ {
			if i.available.BitSet(int(block)) {
				i.blockDataLock.Lock()
				bd, ok := i.blockData[block]
				if !ok {
					bd = &volatilityData{log: make([]int64, 0)}
					i.blockData[block] = bd
				}
				bd.Add(i.expiry)
				i.blockDataLock.Unlock()
			}
			// Always update the total
			i.totalData.Add(i.expiry) // Add to the total volatility counter
		}
	}

	return n, err
}

func (i *VolatilityMonitor) Flush() error {
	return i.prov.Flush()
}

func (i *VolatilityMonitor) Size() uint64 {
	return i.prov.Size()
}

func (i *VolatilityMonitor) Close() error {
	return i.prov.Close()
}

func (i *VolatilityMonitor) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
