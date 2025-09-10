package waitingcache

import (
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/bitfield"
)

type Local struct {
	storage.ProviderWithEvents
	wc         *WaitingCache
	available  bitfield.Bitfield
	NeedAt     func(offset int64, length int32)
	DontNeedAt func(offset int64, length int32)
}

// Relay events to embedded StorageProvider
func (wcl *Local) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := wcl.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(wcl.wc.prov, eventType, eventData)...)
}

func (wcl *Local) GetMetrics() *Metrics {
	return wcl.wc.GetMetrics()
}

func (wcl *Local) ReadAt(buffer []byte, offset int64) (int, error) {
	if wcl.wc.logger != nil {
		wcl.wc.logger.Trace().
			Str("uuid", wcl.wc.uuid.String()).
			Int64("offset", offset).
			Int("length", len(buffer)).
			Msg("local ReadAt")
		defer wcl.wc.logger.Trace().
			Str("uuid", wcl.wc.uuid.String()).
			Int64("offset", offset).
			Int("length", len(buffer)).
			Msg("local ReadAt complete")
	}
	end := uint64(offset + int64(len(buffer)))
	if end > wcl.wc.size {
		end = wcl.wc.size
	}

	bStart := uint(offset / int64(wcl.wc.blockSize))
	bEnd := uint((end-1)/uint64(wcl.wc.blockSize)) + 1

	// WAIT until all the data is available.
	wcl.wc.waitForBlocks(bStart, bEnd, func(b uint) {
		// This is called when we are the FIRST reader for a block.
		// This essentially dedupes for heavy read tasks - only a single NeedAt per block will get sent.
		wcl.NeedAt(int64(b)*int64(wcl.wc.blockSize), int32(wcl.wc.blockSize))
	})
	return wcl.wc.prov.ReadAt(buffer, offset)
}

func (wcl *Local) WriteAt(buffer []byte, offset int64) (int, error) {
	if wcl.wc.logger != nil {
		wcl.wc.logger.Trace().
			Str("uuid", wcl.wc.uuid.String()).
			Int64("offset", offset).
			Int("length", len(buffer)).
			Msg("local WriteAt")
		defer wcl.wc.logger.Trace().
			Str("uuid", wcl.wc.uuid.String()).
			Int64("offset", offset).
			Int("length", len(buffer)).
			Msg("local WriteAt complete")
	}

	end := uint64(offset + int64(len(buffer)))
	if end > wcl.wc.size {
		end = wcl.wc.size
	}

	bStart := uint(offset / int64(wcl.wc.blockSize))
	bEnd := uint((end-1)/uint64(wcl.wc.blockSize)) + 1

	// If the first block is incomplete, we need to wait for it from remote
	if offset > (int64(bStart) * int64(wcl.wc.blockSize)) {
		wcl.wc.waitForBlock(bStart, func(_ uint) {})
		bStart++
	}
	// If the last block is incomplete, we need to wait for it from remote
	if (end % uint64(wcl.wc.blockSize)) > 0 {
		wcl.wc.waitForBlock(bEnd-1, func(_ uint) {})
		bEnd--
	}

	wcl.wc.writeLock.Lock()
	n, err := wcl.wc.prov.WriteAt(buffer, offset)
	if err == nil {
		// Mark the middle blocks as got, and notify the remote we no longer need them
		if bEnd > bStart {
			num := int32(0)
			for b := bStart; b < bEnd; b++ {
				wcl.wc.markAvailableLocalBlock(b)
				num += int32(wcl.wc.blockSize)
			}
			wcl.DontNeedAt(int64(bStart)*int64(wcl.wc.blockSize), num)
		}
	}
	wcl.wc.writeLock.Unlock()
	return n, err
}

func (wcl *Local) Availability() (int, int) {
	numBlocks := (int(wcl.wc.prov.Size()) + wcl.wc.blockSize - 1) / wcl.wc.blockSize
	return wcl.wc.remote.available.Count(0, uint(numBlocks)), numBlocks
}

func (wcl *Local) Flush() error {
	return wcl.wc.prov.Flush()
}

func (wcl *Local) Size() uint64 {
	return wcl.wc.prov.Size()
}

func (wcl *Local) Close() error {
	return wcl.wc.prov.Close()
}

func (wcl *Local) DirtyBlocks(blocks []uint) {
	for _, v := range blocks {
		wcl.wc.markUnavailableRemoteBlock(v)
	}
}

func (wcl *Local) CancelWrites(offset int64, length int64) {
	wcl.wc.prov.CancelWrites(offset, length)
}
