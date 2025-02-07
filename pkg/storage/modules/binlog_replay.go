package modules

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
)

type BinLogReplay struct {
	prov       storage.Provider
	fp         *os.File
	r          io.Reader
	gzipReader *gzip.Reader
	ctime      time.Time
	setCtime   bool
}

func NewBinLogReplay(filename string, prov storage.Provider) (*BinLogReplay, error) {
	var r io.Reader
	var gzipReader *gzip.Reader
	fp, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	r = fp
	// If the filename ends with .gz, use gzip to decompress as we go.
	if strings.HasSuffix(filename, ".gz") {
		gzipReader, err = gzip.NewReader(fp)
		if err != nil {
			return nil, err
		}
		r = gzipReader
	}

	return &BinLogReplay{
		fp:         fp,
		gzipReader: gzipReader,
		r:          r,
		ctime:      time.Now(),
		setCtime:   true,
		prov:       prov,
	}, nil
}

func (i *BinLogReplay) Close() error {
	var err error
	if i.gzipReader != nil {
		err = i.gzipReader.Close()
	}
	return errors.Join(err, i.fp.Close())
}

/**
 * Execute the next read/write.
 * If speed is 0 then it's executed immediately, else we wait.
 */
func (i *BinLogReplay) Next(speed float64, execute bool) (error, error) {
	// Read a header
	header := make([]byte, 12)
	_, err := io.ReadFull(i.r, header)
	if err != nil {
		return nil, err
	}

	if i.setCtime {
		i.ctime = time.Now()
		i.setCtime = false
	}

	length := binary.LittleEndian.Uint32(header[8:])

	data := make([]byte, length)
	_, err = io.ReadFull(i.r, data)
	if err != nil {
		return nil, err
	}

	// If we need to, we'll wait here until the next log should be replayed.
	targetDT := time.Duration(binary.LittleEndian.Uint64(header))
	replayDT := time.Since(i.ctime)
	delay := speed * float64(targetDT-replayDT)
	if delay > 0 {
		time.Sleep(time.Duration(delay))
	}

	if execute {
		// Dispatch the command
		if data[0] == packets.CommandReadAt {
			offset, length, err := packets.DecodeReadAt(data)
			if err != nil {
				return nil, err
			}
			buffer := make([]byte, length)
			_, err = i.prov.ReadAt(buffer, offset)
			return err, nil
		} else if data[0] == packets.CommandWriteAt {
			offset, buffer, err := packets.DecodeWriteAt(data)
			if err != nil {
				return nil, err
			}
			_, err = i.prov.WriteAt(buffer, offset)
			return err, nil
		}
		return nil, errors.New("unknown packet in binlog")
	}
	return nil, nil
}
