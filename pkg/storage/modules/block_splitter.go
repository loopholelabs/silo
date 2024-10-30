package modules

import (
	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 *
 */

type BlockSplitter struct {
	storage.StorageProviderWithEvents
	prov      storage.StorageProvider
	blockSize int
	size      uint64
}

// Relay events to embedded StorageProvider
func (i *BlockSplitter) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.StorageProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewBlockSplitter(prov storage.StorageProvider, blockSize int) *BlockSplitter {
	return &BlockSplitter{
		prov:      prov,
		blockSize: blockSize,
		size:      prov.Size(),
	}
}

func (i *BlockSplitter) ReadAt(buffer []byte, offset int64) (n int, err error) {
	// Split the read up into blocks, and concurrenty perform the reads...
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	bStart := uint(offset / int64(i.blockSize))
	bEnd := uint((end-1)/uint64(i.blockSize)) + 1

	blocks := bEnd - bStart
	errs := make(chan error, blocks)

	for b := bStart; b < bEnd; b++ {
		go func(block_no uint) {
			blockOffset := int64(block_no) * int64(i.blockSize)
			var err error
			if blockOffset > offset {
				// Partial read at the end
				if len(buffer[blockOffset-offset:]) < i.blockSize {
					blockBuffer := make([]byte, i.blockSize)
					_, err = i.prov.ReadAt(blockBuffer, blockOffset)
					copy(buffer[blockOffset-offset:], blockBuffer)
				} else {
					// Complete read in the middle
					s := blockOffset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = i.prov.ReadAt(buffer[s:e], blockOffset)
				}
			} else {
				// Partial read at the start
				blockBuffer := make([]byte, i.blockSize)
				_, err = i.prov.ReadAt(blockBuffer, blockOffset)
				copy(buffer, blockBuffer[offset-blockOffset:])
			}
			errs <- err
		}(b)
	}

	// Wait for completion, Check for errors and return...
	for b := bStart; b < bEnd; b++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
	}

	return len(buffer), nil
}

func (i *BlockSplitter) WriteAt(buffer []byte, offset int64) (n int, err error) {
	// Split the read up into blocks, and concurrenty perform the reads...
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	bStart := uint(offset / int64(i.blockSize))
	bEnd := uint((end-1)/uint64(i.blockSize)) + 1

	blocks := bEnd - bStart
	errs := make(chan error, blocks)

	for b := bStart; b < bEnd; b++ {
		go func(block_no uint) {
			blockOffset := int64(block_no) * int64(i.blockSize)
			var err error
			if blockOffset > offset {
				// Partial write at the end
				if len(buffer[blockOffset-offset:]) < i.blockSize {
					blockBuffer := make([]byte, i.blockSize)
					// Read existing data
					_, err = i.prov.ReadAt(blockBuffer, blockOffset)
					if err == nil {
						// Update the data
						copy(blockBuffer, buffer[blockOffset-offset:])
						// Write back
						_, err = i.prov.WriteAt(blockBuffer, blockOffset)
					}
				} else {
					// Complete write in the middle
					s := blockOffset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = i.prov.WriteAt(buffer[s:e], blockOffset)
				}
			} else {
				// Partial write at the start
				blockBuffer := make([]byte, i.blockSize)
				_, err = i.prov.ReadAt(blockBuffer, blockOffset)
				if err == nil {
					copy(blockBuffer[offset-blockOffset:], buffer)
					_, err = i.prov.WriteAt(blockBuffer, blockOffset)
				}
			}
			errs <- err
		}(b)
	}

	// Wait for completion, Check for errors and return...
	for b := bStart; b < bEnd; b++ {
		e := <-errs
		if e != nil {
			return 0, e
		}
	}

	return len(buffer), nil
}

func (i *BlockSplitter) Flush() error {
	return i.prov.Flush()
}

func (i *BlockSplitter) Size() uint64 {
	return i.prov.Size()
}

func (i *BlockSplitter) Close() error {
	return i.prov.Close()
}

func (i *BlockSplitter) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
