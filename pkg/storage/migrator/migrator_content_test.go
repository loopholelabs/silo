package migrator

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"io"
	"sync"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/protocol/packets"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

/**
 * Test a simple migration but with a content check
 *
 */
func TestMigratorSimpleContentCheck(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	_, err := crand.Read(buffer)
	assert.NoError(t, err)

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	destStorage := sources.NewMemoryStorage(size)

	// Move some data manually first. (We've already got this somehow)
	already_got := blockSize * 5

	bb := make([]byte, blockSize)
	_, err = sourceStorage.ReadAt(bb, int64(blockSize*5))
	assert.NoError(t, err)
	_, err = destStorage.WriteAt(bb, int64(blockSize*5))
	assert.NoError(t, err)

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock

	// Signal that we don't need certain bits
	conf.Dest_content_check = func(offset int, data []byte) bool {
		return offset == already_got
	}

	mig, err := NewMigrator(sourceDirtyRemote,
		destStorage,
		orderer,
		conf)

	assert.NoError(t, err)

	err = mig.Migrate(num_blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)
}

/**
 * Test a simple migration through a pipe. Uses AlternateSource, Hashes WriteAtHash
 *
 */
func TestMigratorSimpleAlternateSourcePipe(t *testing.T) {
	size := 1024 * 1024
	blockSize := 4096
	num_blocks := (size + blockSize - 1) / blockSize

	sourceStorageMem := sources.NewMemoryStorage(size)
	sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
	sourceStorage := modules.NewLockable(sourceDirtyLocal)

	// Set up some data here.
	buffer := make([]byte, size)
	_, err := crand.Read(buffer)
	assert.NoError(t, err)

	n, err := sourceStorage.WriteAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), n)

	orderer := blocks.NewAnyBlockOrder(num_blocks, nil)
	orderer.AddAll()

	// START moving data from sourceStorage to destStorage

	var dest_setup sync.WaitGroup
	var destStorage storage.StorageProvider
	var destFrom *protocol.FromProtocol

	// Create a simple pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	var write_at_hash sync.WaitGroup
	write_at_hash.Add(1)

	dest_setup.Add(1)
	initDev := func(ctx context.Context, p protocol.Protocol, dev uint32) {
		destStorageFactory := func(di *packets.DevInfo) storage.StorageProvider {
			destStorage = sources.NewMemoryStorage(int(di.Size))
			return destStorage
		}

		// Pipe from the protocol to destWaiting
		destFrom = protocol.NewFromProtocol(ctx, dev, destStorageFactory, p)
		dest_setup.Done()
		go func() {
			_ = destFrom.HandleReadAt()
		}()
		go func() {
			_ = destFrom.HandleWriteAt()
		}()
		go func() {
			_ = destFrom.HandleWriteAtHash(func(offset int64, length int64, hash []byte) []byte {
				block := uint(offset) / uint(blockSize)
				assert.Equal(t, uint(1), block)
				write_at_hash.Done()
				return buffer[offset : offset+length]
			})
		}()
		go func() {
			_ = destFrom.HandleDevInfo()
		}()
	}

	prSource := protocol.NewProtocolRW(context.TODO(), []io.Reader{r1}, []io.Writer{w2}, nil)
	prDest := protocol.NewProtocolRW(context.TODO(), []io.Reader{r2}, []io.Writer{w1}, initDev)

	go func() {
		_ = prSource.Handle()
	}()
	go func() {
		_ = prDest.Handle()
	}()

	// Pipe a destination to the protocol
	destination := protocol.NewToProtocol(sourceDirtyRemote.Size(), 17, prSource)

	err = destination.SendDevInfo("test", uint32(blockSize), "")
	assert.NoError(t, err)

	var got_hashes map[uint][32]byte
	var wait_hashes sync.WaitGroup
	wait_hashes.Add(1)
	go func() {
		_ = destination.HandleHashes(func(hashes map[uint][32]byte) {
			got_hashes = hashes
			wait_hashes.Done()
		})
	}()

	conf := NewMigratorConfig().WithBlockSize(blockSize)
	conf.Locker_handler = sourceStorage.Lock
	conf.Unlocker_handler = sourceStorage.Unlock
	conf.Dest_content_check = func(offset int, buffer []byte) bool {
		hash := sha256.Sum256(buffer)
		block := offset / blockSize
		if block == 1 {
			// We should have the hash for it here...
			assert.NotNil(t, got_hashes)
			h, ok := got_hashes[uint(block)]
			assert.True(t, ok)
			assert.Equal(t, h, hash)
			// See if we have it
			_, err = destination.WriteAtHash(hash[:], int64(offset), int64(len(buffer)))
			assert.NoError(t, err)
			return true
		}
		return false
	}

	mig, err := NewMigrator(sourceDirtyRemote,
		destination,
		orderer,
		conf)

	assert.NoError(t, err)

	dest_setup.Wait() // Wait until we're setup

	// Tell the remote that we have a single hash.
	hashes := map[uint][32]byte{
		1: sha256.Sum256(buffer[1*blockSize : 2*blockSize]),
	}
	destFrom.SendHashes(hashes)
	// Make sure we got the hash...
	wait_hashes.Wait()
	assert.Equal(t, len(hashes), len(got_hashes))
	assert.Equal(t, hashes[1], got_hashes[1])

	err = mig.Migrate(num_blocks)
	assert.NoError(t, err)

	err = mig.WaitForCompletion()
	assert.NoError(t, err)

	// Make sure it did one WriteAtHash
	write_at_hash.Wait()

	// This will end with migration completed, and consumer Locked.
	eq, err := storage.Equals(sourceStorageMem, destStorage, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	assert.Equal(t, int(sourceStorageMem.Size()), destFrom.GetDataPresent())
}
