package modules

import (
	"crypto/rand"
	"crypto/sha256"
	"os"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/util"
	"github.com/stretchr/testify/assert"
)

func TestCopyOnWriteReads(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	mem := sources.NewMemoryStorage(size)
	cache := sources.NewMemoryStorage(size)

	// Fill it with stuff
	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	_, err = mem.WriteAt(data, 0)
	assert.NoError(t, err)

	cow := NewCopyOnWrite(mem, cache, 10, true, nil)

	// Now try doing some reads...

	buff := make([]byte, 30)
	_, err = cow.ReadAt(buff, 18)
	assert.NoError(t, err)

	// Check the data is correct...
	assert.Equal(t, data[18:48], buff)

	n := cow.exists.Count(0, cow.exists.Length())
	assert.Equal(t, 0, n)

	// Do another read that spans past this one...

	buff2 := make([]byte, 50)
	_, err = cow.ReadAt(buff2, 8)
	assert.NoError(t, err)

	assert.Equal(t, data[8:58], buff2)

	n2 := cow.exists.Count(0, cow.exists.Length())
	assert.Equal(t, 0, n2)

}

func TestCopyOnWriteWrites(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	srcMem := sources.NewMemoryStorage(size)
	mem := NewMetrics(srcMem)
	srcCache := sources.NewMemoryStorage(size)
	cache := NewMetrics(srcCache)

	// Fill it with stuff
	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	_, err = srcMem.WriteAt(data, 0)
	assert.NoError(t, err)

	cow := NewCopyOnWrite(mem, cache, 10, true, nil)

	// Now try doing some writes...

	buff := make([]byte, 30)
	_, err = cow.WriteAt(buff, 18)
	assert.NoError(t, err)

	n := cow.exists.Count(0, cow.exists.Length())
	assert.Equal(t, 4, n)

	// Read some back make sure it looks ok...

	buff2 := make([]byte, 100)
	_, err = cow.ReadAt(buff2, 0)
	assert.NoError(t, err)

	buff3 := make([]byte, 100)
	// Read from src
	copy(buff3, data)
	// Merge in write
	copy(buff3[18:], buff)

	assert.Equal(t, buff3, buff2)
}

func TestCopyOnWriteReadOverrun(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	srcMem := sources.NewMemoryStorage(size)
	mem := NewMetrics(srcMem)
	srcCache := sources.NewMemoryStorage(size)
	cache := NewMetrics(srcCache)

	// Fill it with stuff
	data := make([]byte, size+10) // Some extra
	_, err := rand.Read(data)
	assert.NoError(t, err)
	onlydata := data[:size]
	n, err := srcMem.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	cow := NewCopyOnWrite(mem, cache, 1024, true, nil)

	buff2 := make([]byte, 100)
	n, err = cow.ReadAt(buff2, int64(size-50))

	assert.NoError(t, err)
	assert.Equal(t, 50, n)

	assert.Equal(t, onlydata[size-50:], buff2[:50])
}

func TestCopyOnWriteReadOverrunNonMultiple(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	srcMem := sources.NewMemoryStorage(size)
	mem := NewMetrics(srcMem)
	srcCache := sources.NewMemoryStorage(size)
	cache := NewMetrics(srcCache)

	// Fill it with stuff
	data := make([]byte, size+10)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	onlydata := data[:size]

	cow := NewCopyOnWrite(mem, cache, 1000, true, nil) // NB 1024*1024 isn't multiple of 1000 blocksize

	n, err := cow.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	buff2 := make([]byte, 100)
	n, err = cow.ReadAt(buff2, int64(size-50))

	assert.NoError(t, err)
	assert.Equal(t, 50, n)

	assert.Equal(t, onlydata[size-50:], buff2[:50])
}

func TestCopyOnWriteCRCIssue(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 16 * 1024
	blockSize := 64 * 1024

	fstore, err := sources.NewFileStorageSparseCreate("test_data_sparse", uint64(size), blockSize)
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.Remove("test_data_sparse")
	})

	rosource := sources.NewMemoryStorage(size)

	cow := NewCopyOnWrite(rosource, fstore, blockSize, true, nil)

	reference := sources.NewMemoryStorage(size)

	// Fill it with random stuff
	data := make([]byte, size)
	_, err = rand.Read(data)
	assert.NoError(t, err)
	_, err = rosource.WriteAt(data, 0)
	assert.NoError(t, err)
	_, err = reference.WriteAt(data, 0)
	assert.NoError(t, err)

	// Now do some WriteAt() calls, and then a big ReadAt() and make sure they match

	doWrite := func(offset int64) {
		buffer := make([]byte, 4096)
		_, err := rand.Read(buffer)
		assert.NoError(t, err)

		n, err := cow.WriteAt(buffer, offset)
		assert.NoError(t, err)
		assert.Equal(t, n, 4096)

		n, err = reference.WriteAt(buffer, offset)
		assert.NoError(t, err)
		assert.Equal(t, n, 4096)
	}

	doWrite(8192)
	doWrite(0)
	doWrite(4096)

	// Now check they are equal

	eq, err := storage.Equals(reference, fstore, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	eq, err = storage.Equals(reference, cow, blockSize)
	assert.NoError(t, err)
	assert.True(t, eq)

	buffCow := make([]byte, cow.Size())
	_, err = cow.ReadAt(buffCow, 0)
	assert.NoError(t, err)

	buffRef := make([]byte, reference.Size())
	_, err = reference.ReadAt(buffRef, 0)
	assert.NoError(t, err)
}

func TestCopyOnWriteClose(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	mem := sources.NewMemoryStorage(size)
	cache := sources.NewMemoryStorage(size)

	// Make reads take a little while...
	memHooks := NewHooks(mem)
	memHooks.PreRead = func(_ []byte, _ int64) (bool, int, error) {
		time.Sleep(100 * time.Millisecond)
		return true, 0, nil
	}

	cow := NewCopyOnWrite(memHooks, cache, 10, true, nil)

	// ReadAt1
	go func() {
		buff := make([]byte, 30)
		_, err := cow.ReadAt(buff, 18)
		assert.NoError(t, err)
	}()

	// ReadAt2
	go func() {
		time.Sleep(75 * time.Millisecond)
		buff := make([]byte, 30)
		_, err := cow.ReadAt(buff, 18)
		assert.ErrorIs(t, err, ErrClosed) // We'd expect there to be an error here
	}()

	time.Sleep(50 * time.Millisecond)

	// Here, ReadAt1 should still be in progress...
	// The ReadAt2 should happen during this close, which will cause a panic.
	err := cow.Close()
	assert.NoError(t, err)
}

func TestCopyOnWriteReadsNonzero(t *testing.T) {

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	mem := sources.NewMemoryStorage(size)
	cache := sources.NewMemoryStorage(size)

	// Fill it with stuff
	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	_, err = mem.WriteAt(data, 0)
	assert.NoError(t, err)

	nonzero := util.NewBitfield((1024*1024 + 10 - 1) / 10)
	// Set some bits
	nonzero.SetBits(0, nonzero.Length()/2)

	hashes := make([][sha256.Size]byte, 0)
	numBlocks := (size + 10 - 1) / 10
	zeroHash := [sha256.Size]byte(make([]byte, sha256.Size))
	hash := make([]byte, sha256.Size)

	// Setup some dummy hash data, only really matters if it's zeroes or not for now...
	for b := 0; b < numBlocks; b++ {
		if b < (numBlocks / 2) {
			rand.Read(hash)
			hashes = append(hashes, [sha256.Size]byte(hash))
		} else {
			hashes = append(hashes, zeroHash)
		}
	}

	cow := NewCopyOnWrite(mem, cache, 10, true, hashes)

	// Now try doing some reads...

	buff := make([]byte, 30)
	_, err = cow.ReadAt(buff, 18)
	assert.NoError(t, err)

	// Check the data is correct...
	assert.Equal(t, data[18:48], buff)

	// Do another read in the other half

	buff2 := make([]byte, 50)
	_, err = cow.ReadAt(buff2, 512*1024+89)
	assert.NoError(t, err)

	zerobytes := make([]byte, 50)

	assert.Equal(t, zerobytes, buff2)

	// Check metrics
	met := cow.GetMetrics()

	// There should have been some zero reads
	assert.Greater(t, met.MetricZeroReadOps, uint64(0))
	assert.Greater(t, met.MetricZeroReadBytes, uint64(0))

	// Now try a write...

	buff3 := make([]byte, 30)
	_, err = cow.WriteAt(buff3, 512*1024+89)
	assert.NoError(t, err)

	// Check metrics
	met = cow.GetMetrics()

	// There should have been some zero reads
	assert.Greater(t, met.MetricZeroPreWriteReadOps, uint64(0))
	assert.Greater(t, met.MetricZeroPreWriteReadBytes, uint64(0))

}

/**
 * This test checks that CoW is doing things in an optimized way
 * We don't want to read or write more than we need to.
 */
func TestCopyOnWriteOptimized(t *testing.T) {
	blockSize := 1024

	// Create a new block storage, backed by memory storage
	size := 1024 * 1024
	mem := sources.NewMemoryStorage(size)
	metMem := NewMetrics(mem)
	cache := sources.NewMemoryStorage(size)
	metCache := NewMetrics(cache)

	// Fill it with stuff
	data := make([]byte, size)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	_, err = mem.WriteAt(data, 0)
	assert.NoError(t, err)

	cow := NewCopyOnWrite(metMem, metCache, blockSize, true, nil)

	// Do a single small write to the CoW.
	// This should result in a read of a block from the ROsrc,
	// Then a block write to the cache.

	smallData := make([]byte, 16)
	n, err := cow.WriteAt(smallData, 40) // Block 0
	assert.NoError(t, err)
	assert.Equal(t, len(smallData), n)

	mCache := metCache.GetMetrics()
	mMem := metMem.GetMetrics()

	// We should expect to have to read a block, and write a block.
	assert.Equal(t, uint64(blockSize), mMem.ReadBytes)
	assert.Equal(t, uint64(0), mMem.WriteBytes)

	assert.Equal(t, uint64(blockSize), mCache.WriteBytes)
	assert.Equal(t, uint64(0), mCache.ReadBytes)

	// Now lets overwrite on that same block.
	// This time, the block is already in our cache, so we should ONLY need to write n bytes

	n, err = cow.WriteAt(smallData, 30) // Block 0
	assert.NoError(t, err)
	assert.Equal(t, len(smallData), n)

	mCache = metCache.GetMetrics()
	mMem = metMem.GetMetrics()

	assert.Equal(t, uint64(blockSize), mMem.ReadBytes)
	assert.Equal(t, uint64(0), mMem.WriteBytes)

	assert.Equal(t, uint64(blockSize+n), mCache.WriteBytes)
	assert.Equal(t, uint64(0), mCache.ReadBytes)

	// Check that a read is efficient as well

	somedata := make([]byte, 16)

	n, err = cow.ReadAt(somedata, 20) // Block 0, so it's in the cache
	assert.NoError(t, err)
	assert.Equal(t, len(somedata), n)

	mCache = metCache.GetMetrics()
	mMem = metMem.GetMetrics()

	assert.Equal(t, uint64(blockSize), mMem.ReadBytes)
	assert.Equal(t, uint64(0), mMem.WriteBytes)

	assert.Equal(t, uint64(blockSize+n), mCache.WriteBytes)
	assert.Equal(t, uint64(len(somedata)), mCache.ReadBytes)

}
