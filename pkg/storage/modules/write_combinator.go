package modules

import (
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

// Restrictions
// * All WriteAts should have a block aligned offset.
// * WriteAt buffer size should be block multiple, unless it includes the END of the storage, in which case can be less.
// * priority should be unique amongst sources.

type WriteCombinator struct {
	prov      storage.Provider
	blockSize int
	numBlocks int
	size      uint64
	writeLock sync.Mutex
	sources   map[int]*writeSource
}

// Create a new combinator
func NewWriteCombinator(prov storage.Provider, blockSize int) *WriteCombinator {
	numBlocks := (prov.Size() + uint64(blockSize) - 1) / uint64(blockSize)
	return &WriteCombinator{
		prov:      prov,
		blockSize: blockSize,
		size:      prov.Size(),
		numBlocks: int(numBlocks),
		sources:   make(map[int]*writeSource, 0),
	}
}

// Add a new source to write into the combinator, with specified priority. Priority must be unique.
func (i *WriteCombinator) AddSource(priority int) storage.Provider {
	i.writeLock.Lock()
	defer i.writeLock.Unlock()
	ws := &writeSource{
		priority:   priority,
		combinator: i,
		available:  util.NewBitfield(i.numBlocks),
	}
	i.sources[priority] = ws
	return ws
}

// Remove a source from this combinator.
func (i *WriteCombinator) RemoveSource(priority int) {
	i.writeLock.Lock()
	defer i.writeLock.Unlock()
	delete(i.sources, priority)
}

// Find the highest priority write for a block, or -1 if no writes
func (i *WriteCombinator) getHighestPriorityForBlock(b uint) int {
	highestPriority := -1
	for _, ws := range i.sources {
		if ws.priority > highestPriority && ws.available.BitSet(int(b)) {
			highestPriority = ws.priority
		}
	}
	return highestPriority
}

type writeSource struct {
	storage.ProviderWithEvents
	priority   int
	combinator *WriteCombinator
	available  *util.Bitfield
}

// Relay events to embedded StorageProvider
func (ws *writeSource) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := ws.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(ws.combinator.prov, eventType, eventData)...)
}

// Writes only allowed through if they beat any existing writes
func (ws *writeSource) WriteAt(buffer []byte, offset int64) (int, error) {
	ws.combinator.writeLock.Lock()
	defer ws.combinator.writeLock.Unlock()

	end := uint64(offset + int64(len(buffer)))
	if end > ws.combinator.size {
		end = ws.combinator.size
	}

	bStart := uint(offset / int64(ws.combinator.blockSize))
	bEnd := uint((end-1)/uint64(ws.combinator.blockSize)) + 1

	blockOffset := int64(0)

	// Check block by block if we should let it through...
	for b := bStart; b < bEnd; b++ {
		existingPriority := ws.combinator.getHighestPriorityForBlock(b)
		if ws.priority > existingPriority {
			// Allow the write through, and update our availability
			blockEnd := blockOffset + int64(ws.combinator.blockSize)
			if blockEnd > int64(ws.combinator.size) {
				blockEnd = int64(ws.combinator.size)
			}
			blockData := buffer[blockOffset:blockEnd]
			_, err := ws.combinator.prov.WriteAt(blockData, offset+blockOffset)
			if err != nil {
				return 0, err
			}
			ws.available.SetBit(int(b))
		}
		blockOffset += int64(ws.combinator.blockSize)
	}

	// Report no error.
	return len(buffer), nil
}

// Route everything else through to prov
func (ws *writeSource) ReadAt(buffer []byte, offset int64) (int, error) {
	return ws.combinator.prov.ReadAt(buffer, offset)
}

func (ws *writeSource) Flush() error {
	return ws.combinator.prov.Flush()
}

func (ws *writeSource) Size() uint64 {
	return ws.combinator.prov.Size()
}

func (ws *writeSource) Close() error {
	return ws.combinator.prov.Close()
}

func (ws *writeSource) CancelWrites(offset int64, length int64) {
	ws.combinator.prov.CancelWrites(offset, length)
}