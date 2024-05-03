package modules

import (
	"encoding/binary"
	"os"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

type BinLogReplay struct {
	prov      storage.StorageProvider
	fp        *os.File
	ctime     time.Time
	set_ctime bool
}

func NewBinLogReplay(filename string, prov storage.StorageProvider) (*BinLogReplay, error) {
	fp, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return &BinLogReplay{
		fp:        fp,
		ctime:     time.Now(),
		set_ctime: true,
		prov:      prov,
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

	if i.set_ctime {
		i.ctime = time.Now()
		i.set_ctime = false
	}

	// If we need to, we'll wait here until the next log should be replayed.
	target_dt := time.Duration(binary.LittleEndian.Uint64(header))
	replay_dt := time.Since(i.ctime)
	delay := speed * float64(target_dt-replay_dt)
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
	if data[0] == packets.COMMAND_READ_AT {
		offset, length, err := packets.DecodeReadAt(data)
		if err != nil {
			return err
		}
		buffer := make([]byte, length)
		_, err = i.prov.ReadAt(buffer, offset)
		return err
	} else if data[0] == packets.COMMAND_WRITE_AT {
		offset, buffer, err := packets.DecodeWriteAt(data)
		if err != nil {
			return err
		}
		_, err = i.prov.WriteAt(buffer, offset)
		return err
	} else {
		panic("Unknown packet in binlog")
	}
}
