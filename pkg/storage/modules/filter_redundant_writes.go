package modules

import (
	"io"

	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 * This filters out writes that aren't necessary based on a source. It can also compress writes to only areas that changed
 *
 */

type FilterRedundantWrites struct {
	storage.ProviderWithEvents
	prov              storage.Provider
	source            io.ReaderAt
	noChangeAllowance int
}

// Relay events to embedded StorageProvider
func (i *FilterRedundantWrites) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewFilterRedundantWrites(prov storage.Provider, source io.ReaderAt, allowance int) *FilterRedundantWrites {
	return &FilterRedundantWrites{
		prov:              prov,
		source:            source,
		noChangeAllowance: allowance,
	}
}

func (i *FilterRedundantWrites) ReadAt(buffer []byte, offset int64) (int, error) {
	return i.prov.ReadAt(buffer, offset)
}

func (i *FilterRedundantWrites) WriteAt(buffer []byte, offset int64) (int, error) {
	// Compare data has actually changed...
	original := make([]byte, len(buffer))
	n, e := i.source.ReadAt(original, offset)
	if n != len(original) || e != nil {
		// The source doesn't know, so we'll have to pass it on as is
		return i.prov.WriteAt(buffer, offset)
	}

	for x := 0; x < len(original); x++ {
		ov := original[x]
		// Find the next start of changed data...
		if buffer[x] != ov {
			// Find the end of the changed data, with an allowance for unchanged data
			writeLen := 1
			noChangeCount := 0
			for y, ov2 := range original[x+1:] {
				writeLen++
				if buffer[x+1+y] != ov2 {
					noChangeCount = 0 // Reset this counter
				} else {
					noChangeCount++
					if noChangeCount > i.noChangeAllowance {
						writeLen -= (noChangeCount - 1)
						break
					}
				}
			}

			// Send on a write operation here...
			n, err := i.prov.WriteAt(buffer[x:x+writeLen], offset+int64(x))
			if n != writeLen || err != nil {
				return 0, err
			}
			x += writeLen
		}
	}

	// Signal no error
	return len(buffer), nil
}

func (i *FilterRedundantWrites) Flush() error {
	return i.prov.Flush()
}

func (i *FilterRedundantWrites) Size() uint64 {
	return i.prov.Size()
}

func (i *FilterRedundantWrites) Close() error {
	return i.prov.Close()
}

func (i *FilterRedundantWrites) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
