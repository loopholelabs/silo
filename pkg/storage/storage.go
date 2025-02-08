package storage

import (
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/util"
)

type Provider interface {
	io.ReaderAt
	io.WriterAt
	Size() uint64
	Flush() error
	Close() error
	CancelWrites(offset int64, length int64)
}

type LockableProvider interface {
	Provider
	Lock()
	Unlock()
}

type TrackingProvider interface {
	Provider
	LockWrites()
	UnlockWrites()
	TrackAt(length int64, offset int64)
	Sync() *util.Bitfield
}

type ExposedStorage interface {
	Init() error
	Shutdown() error
	Device() string
	SetProvider(prov Provider)
}

type BlockOrder interface {
	AddAll()
	Add(block int)
	Remove(block int)
	GetNext() *BlockInfo
}

type BlockInfo struct {
	Block int
	Type  int
}

var BlockInfoFinish = &BlockInfo{Block: -1}

var BlockTypeAny = -1
var BlockTypeStandard = 0
var BlockTypeDirty = 1
var BlockTypePriority = 2

type SyncStartConfig struct {
	AlternateSources []packets.AlternateSource
	Destination      Provider
}

/**
 * Check if two storageProviders hold the same data.
 *
 */
func Equals(sp1 Provider, sp2 Provider, blockSize int) (bool, error) {
	if sp1.Size() != sp2.Size() {
		fmt.Printf("Equals: Size differs (%d %d)\n", sp1.Size(), sp2.Size())
		return false, nil
	}

	size := int(sp1.Size())

	sourceBuff := make([]byte, blockSize)
	destBuff := make([]byte, blockSize)
	for i := 0; i < size; i += blockSize {
		sourceBuff = sourceBuff[:cap(sourceBuff)]
		destBuff = destBuff[:cap(destBuff)]

		n, err := sp1.ReadAt(sourceBuff, int64(i))
		if err != nil {
			fmt.Printf("Equals: sp1.ReadAt %v\n", err)
			return false, err
		}
		sourceBuff = sourceBuff[:n]
		n, err = sp2.ReadAt(destBuff, int64(i))
		if err != nil {
			fmt.Printf("Equals: sp2.ReadAt %v\n", err)
			return false, err
		}
		destBuff = destBuff[:n]
		if len(sourceBuff) != len(destBuff) {
			fmt.Printf("Equals: data len sp1 sp2 %d %d\n", len(sourceBuff), len(destBuff))
			return false, nil
		}
		for j := 0; j < n; j++ {
			if sourceBuff[j] != destBuff[j] {
				fmt.Printf("Equals: Block %d differs [sp1 %d, sp2 %d]\n", i/blockSize, sourceBuff[j], destBuff[j])
				return false, nil
			}
		}
	}

	return true, nil
}

/**
 * Calc the hash of a device
 *
 */
func Hash(sp Provider, blockSize int) ([]byte, error) {
	size := int(sp.Size())

	hasher := sha256.New()

	sourceBuff := make([]byte, blockSize)
	for i := 0; i < size; i += blockSize {
		n, err := sp.ReadAt(sourceBuff, int64(i))
		if err != nil {
			return nil, err
		}
		hasher.Write(sourceBuff[:n])
	}

	return hasher.Sum(nil), nil
}

/**
 * Map a function over blocks within the range.
 *
 */
func MapOverBlocks(offset int64, length int32, blockSize int, f func(b int, complete bool)) {
	end := uint64(offset + int64(length))

	bStart := int(offset / int64(blockSize))
	bEnd := int((end-1)/uint64(blockSize)) + 1
	for b := bStart; b < bEnd; b++ {
		complete := true
		// If the first block is incomplete
		if offset > (int64(bStart) * int64(blockSize)) {
			complete = false
		}
		// If the last block is incomplete
		if (end % uint64(blockSize)) > 0 {
			complete = false
		}

		f(b, complete)
	}
}
