package waitingcache

import (
	"io"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/bitfield"
)

type Remote struct {
	storage.ProviderWithEvents
	wc        *WaitingCache
	available bitfield.Bitfield
}

// Relay events to embedded StorageProvider
func (wcr *Remote) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	if eventType == storage.EventTypeAvailable {
		markRange := eventData.([]int64)
		// offset, length
		offset := markRange[0]
		length := markRange[1]
		end := uint64(offset + length)
		if end > wcr.wc.size {
			end = wcr.wc.size
		}

		bStart := uint(offset / int64(wcr.wc.blockSize))
		bEnd := uint((end-1)/uint64(wcr.wc.blockSize)) + 1

		wcr.wc.markAvailableRemoteBlocks(bStart, bEnd)
	}

	data := wcr.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(wcr.wc.prov, eventType, eventData)...)
}

func (wcr *Remote) GetMetrics() *Metrics {
	return wcr.wc.GetMetrics()
}

func (wcr *Remote) ReadAt(_ []byte, _ int64) (int, error) {
	// Remote reads are unsupported at the moment.
	return 0, io.EOF
}

func (wcr *Remote) WriteAt(buffer []byte, offset int64) (int, error) {
	if wcr.wc.logger != nil {
		wcr.wc.logger.Trace().
			Str("uuid", wcr.wc.uuid.String()).
			Int64("offset", offset).
			Int("length", len(buffer)).
			Msg("remote WriteAt")
		defer wcr.wc.logger.Trace().
			Str("uuid", wcr.wc.uuid.String()).
			Int64("offset", offset).
			Int("length", len(buffer)).
			Msg("remote WriteAt complete")
	}

	end := uint64(offset + int64(len(buffer)))
	if end > wcr.wc.size {
		end = wcr.wc.size
	}

	bStart := uint(offset / int64(wcr.wc.blockSize))
	bEnd := uint((end-1)/uint64(wcr.wc.blockSize)) + 1

	align := 0
	// If the first block is incomplete, we won't mark it.
	if offset > (int64(bStart) * int64(wcr.wc.blockSize)) {
		bStart++
		align = int(offset - (int64(bStart) * int64(wcr.wc.blockSize)))
	}
	// If the last block is incomplete, we won't mark it. *UNLESS* It's the last block in the storage
	if (end % uint64(wcr.wc.blockSize)) > 0 {
		if uint64(offset)+uint64(len(buffer)) < wcr.wc.size {
			bEnd--
		}
	}

	var err error
	var n int

	if wcr.wc.allowLocalWrites {
		// Check if we have local data that needs merging (From local writes)
		avail := wcr.wc.local.available.Collect(bStart, bEnd)

		if len(avail) != 0 {
			pbuffer := make([]byte, len(buffer))
			_, err = wcr.wc.prov.ReadAt(pbuffer, offset)
			if err == nil {
				for _, b := range avail {
					s := align + (int(b-bStart) * wcr.wc.blockSize)
					// Merge the data in. We know these are complete blocks.
					// NB This does modify the callers buffer.
					copy(buffer[s:s+wcr.wc.blockSize], pbuffer[s:s+wcr.wc.blockSize])
				}
			}
		}
	}

	// Perform the WriteAt
	if err == nil {
		n, err = wcr.wc.prov.WriteAt(buffer, offset)
	}

	// Signal that we have blocks available from remote
	if err == nil {
		if bEnd > bStart {
			wcr.wc.markAvailableRemoteBlocks(bStart, bEnd)
		}
	}

	return n, err
}

func (wcr *Remote) Flush() error {
	return wcr.wc.prov.Flush()
}

func (wcr *Remote) Size() uint64 {
	return wcr.wc.prov.Size()
}

func (wcr *Remote) Close() error {
	return wcr.wc.prov.Close()
}

func (wcr *Remote) CancelWrites(offset int64, length int64) {
	wcr.wc.prov.CancelWrites(offset, length)
}
