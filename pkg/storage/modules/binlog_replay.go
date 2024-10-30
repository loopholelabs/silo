package modules

import (
	"encoding/binary"
	"os"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

type BinLogReplay struct {
	prov     storage.Provider
	fp       *os.File
	ctime    time.Time
	setCtime bool
}

func NewBinLogReplay(filename string, prov storage.Provider) (*BinLogReplay, error) {
	fp, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return &BinLogReplay{
		fp:       fp,
		ctime:    time.Now(),
		setCtime: true,
		prov:     prov,
	}, nil
}

/**
 * Execute the next read/write.
 * If speed is 0 then it's executed immediately, else we wait.
 */
func (i *BinLogReplay) ExecuteNext(speed float64) error {
	// Read a header
	header := make([]byte, 12)
	_, err := i.fp.Read(header)
	if err != nil {
		return err
	}

	if i.setCtime {
		i.ctime = time.Now()
		i.setCtime = false
	}

	// If we need to, we'll wait here until the next log should be replayed.
	targetDT := time.Duration(binary.LittleEndian.Uint64(header))
	replayDT := time.Since(i.ctime)
	delay := speed * float64(targetDT-replayDT)
	if delay > 0 {
		time.Sleep(time.Duration(delay))
	}

	length := binary.LittleEndian.Uint32(header[8:])
	data := make([]byte, length)
	_, err = i.fp.Read(data)
	if err != nil {
		return err
	}

	// Dispatch the command
	if data[0] == packets.CommandReadAt {
		offset, length, err := packets.DecodeReadAt(data)
		if err != nil {
			return err
		}
		buffer := make([]byte, length)
		_, err = i.prov.ReadAt(buffer, offset)
		return err
	} else if data[0] == packets.CommandWriteAt {
		offset, buffer, err := packets.DecodeWriteAt(data)
		if err != nil {
			return err
		}
		_, err = i.prov.WriteAt(buffer, offset)
		return err
	}
	panic("Unknown packet in binlog")

}
