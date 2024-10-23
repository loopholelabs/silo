package waitingcache

import (
	"io"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type WaitingCacheRemote struct {
	storage.StorageProviderWithEvents
	wc        *WaitingCache
	available util.Bitfield
}

// Relay events to embedded StorageProvider
func (wcl *WaitingCacheRemote) SendEvent(event_type storage.EventType, event_data storage.EventData) []storage.EventReturnData {
	data := wcl.StorageProviderWithEvents.SendEvent(event_type, event_data)
	return append(data, storage.SendEvent(wcl.wc.prov, event_type, event_data)...)
}

func (wcl *WaitingCacheRemote) ReadAt(buffer []byte, offset int64) (int, error) {
	// Remote reads are unsupported at the moment.
	return 0, io.EOF
}

func (wcl *WaitingCacheRemote) WriteAt(buffer []byte, offset int64) (int, error) {
	end := uint64(offset + int64(len(buffer)))
	if end > wcl.wc.size {
		end = wcl.wc.size
	}

	b_start := uint(offset / int64(wcl.wc.blockSize))
	b_end := uint((end-1)/uint64(wcl.wc.blockSize)) + 1

	align := 0
	// If the first block is incomplete, we won't mark it.
	if offset > (int64(b_start) * int64(wcl.wc.blockSize)) {
		b_start++
		align = int(offset - (int64(b_start) * int64(wcl.wc.blockSize)))
	}
	// If the last block is incomplete, we won't mark it. *UNLESS* It's the last block in the storage
	if (end % uint64(wcl.wc.blockSize)) > 0 {
		if uint64(offset)+uint64(len(buffer)) < wcl.wc.size {
			b_end--
		}
	}

	var err error
	var n int

	if wcl.wc.allowLocalWrites {
		// Check if we have local data that needs merging (From local writes)
		avail := wcl.wc.local.available.Collect(uint(b_start), uint(b_end))

		if len(avail) != 0 {
			pbuffer := make([]byte, len(buffer))
			_, err = wcl.wc.prov.ReadAt(pbuffer, offset)
			if err == nil {
				for _, b := range avail {
					s := align + (int(b-b_start) * wcl.wc.blockSize)
					// Merge the data in. We know these are complete blocks.
					// NB This does modify the callers buffer.
					copy(buffer[s:s+wcl.wc.blockSize], pbuffer[s:s+wcl.wc.blockSize])
				}
			}
		}
	}

	// Perform the WriteAt
	if err == nil {
		n, err = wcl.wc.prov.WriteAt(buffer, offset)
	}

	// Signal that we have blocks available from remote
	if err == nil {
		if b_end > b_start {
			wcl.wc.markAvailableRemoteBlocks(b_start, b_end)
		}
	}

	return n, err
}

func (wcl *WaitingCacheRemote) Flush() error {
	return wcl.wc.prov.Flush()
}

func (wcl *WaitingCacheRemote) Size() uint64 {
	return wcl.wc.prov.Size()
}

func (wcl *WaitingCacheRemote) Close() error {
	return wcl.wc.prov.Close()
}

func (wcl *WaitingCacheRemote) CancelWrites(offset int64, length int64) {
	wcl.wc.prov.CancelWrites(offset, length)
}
