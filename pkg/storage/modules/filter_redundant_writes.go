package modules

import (
	"io"

	"github.com/loopholelabs/silo/pkg/storage"
)

type FilterRedundantWrites struct {
	prov                storage.StorageProvider
	source              io.ReaderAt
	no_change_allowance int
}

func NewFilterRedundantWrites(prov storage.StorageProvider, source io.ReaderAt, allowance int) *FilterRedundantWrites {
	return &FilterRedundantWrites{
		prov:                prov,
		source:              source,
		no_change_allowance: allowance,
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
			write_len := 1
			no_change_count := 0
			for y, ov2 := range original[x+1:] {
				write_len++
				if buffer[x+1+y] != ov2 {
					no_change_count = 0 // Reset this counter
				} else {
					no_change_count++
					if no_change_count > i.no_change_allowance {
						write_len -= (no_change_count - 1)
						break
					}
				}
			}

			// Send on a write operation here...
			n, err := i.prov.WriteAt(buffer[x:x+write_len], offset+int64(x))
			if n != write_len || err != nil {
				return 0, err
			}
			x += write_len
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
