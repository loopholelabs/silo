package sources

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (

	//nolint:all
	errNoSuchKey = errors.New("The specified key does not exist.") // Minio doesn't export errors
)

type S3Storage struct {
	storage.ProviderWithEvents
	dummy                    bool
	client                   *minio.Client
	bucket                   string
	prefix                   string
	size                     uint64
	blockSize                int
	lockers                  []sync.RWMutex
	contexts                 []context.CancelFunc
	contextsLock             sync.Mutex
	metricsBlocksWCount      uint64
	metricsBlocksWDataBytes  uint64
	metricsBlocksWBytes      uint64
	metricsBlocksWTimeNS     uint64
	metricsBlocksWPreRCount  uint64
	metricsBlocksWPreRBytes  uint64
	metricsBlocksWPreRTimeNS uint64
	metricsBlocksRCount      uint64
	metricsBlocksRDataBytes  uint64
	metricsBlocksRBytes      uint64
	metricsBlocksRTimeNS     uint64
}

func NewS3Storage(secure bool, endpoint string,
	access string,
	secretAccess string,
	bucket string,
	prefix string,
	size uint64,
	blockSize int) (*S3Storage, error) {

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secretAccess, ""),
		Secure: secure,
	})

	if err != nil {
		return nil, err
	}

	numBlocks := (int(size) + blockSize - 1) / blockSize

	return &S3Storage{
		size:      size,
		blockSize: blockSize,
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
		lockers:   make([]sync.RWMutex, numBlocks),
		contexts:  make([]context.CancelFunc, numBlocks),
	}, nil
}

func NewS3StorageDummy(size uint64,
	blockSize int) (*S3Storage, error) {

	numBlocks := (int(size) + blockSize - 1) / blockSize

	return &S3Storage{
		size:      size,
		blockSize: blockSize,
		dummy:     true,
		lockers:   make([]sync.RWMutex, numBlocks),
		contexts:  make([]context.CancelFunc, numBlocks),
	}, nil
}

func NewS3StorageCreate(secure bool, endpoint string,
	access string,
	secretAccess string,
	bucket string,
	prefix string,
	size uint64,
	blockSize int) (*S3Storage, error) {

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secretAccess, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}

	exists, err := client.BucketExists(context.TODO(), bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		// If the bucket doesn't exist, Create the bucket...
		err = client.MakeBucket(context.TODO(), bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}

	numBlocks := (int(size) + blockSize - 1) / blockSize

	return &S3Storage{
		size:      size,
		blockSize: blockSize,
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
		lockers:   make([]sync.RWMutex, numBlocks),
		contexts:  make([]context.CancelFunc, numBlocks),
	}, nil
}

func (i *S3Storage) setContext(block int, cancel context.CancelFunc) {
	i.contextsLock.Lock()
	ex := i.contexts[block]
	if ex != nil {
		ex() // Cancel any existing context for this block
	}
	i.contexts[block] = cancel // Set to the new context
	i.contextsLock.Unlock()
}

func (i *S3Storage) ReadAt(buffer []byte, offset int64) (int, error) {
	// Split the read up into blocks, and concurrenty perform the reads...
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	bStart := uint(offset / int64(i.blockSize))
	bEnd := uint((end-1)/uint64(i.blockSize)) + 1

	blocks := bEnd - bStart
	errs := make(chan error, blocks)

	getData := func(buff []byte, off int64) (int, error) {
		if i.dummy {
			atomic.AddUint64(&i.metricsBlocksRCount, 1)
			atomic.AddUint64(&i.metricsBlocksRBytes, uint64(len(buff)))
			return len(buff), nil
		}
		i.lockers[off/int64(i.blockSize)].RLock()
		ctime := time.Now()
		obj, err := i.client.GetObject(context.TODO(), i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
		i.lockers[off/int64(i.blockSize)].RUnlock()
		if err != nil {
			if err.Error() == errNoSuchKey.Error() {
				return len(buff), nil
			}
			return 0, err
		}
		n, err := obj.Read(buff)
		dtime := time.Since(ctime)
		if err == io.EOF {
			atomic.AddUint64(&i.metricsBlocksRCount, 1)
			atomic.AddUint64(&i.metricsBlocksRBytes, uint64(n))
			atomic.AddUint64(&i.metricsBlocksRTimeNS, uint64(dtime.Nanoseconds()))
		} else if err.Error() == errNoSuchKey.Error() {
			return len(buff), nil
		}

		return n, err
	}

	for b := bStart; b < bEnd; b++ {
		go func(blockNo uint) {
			blockOffset := int64(blockNo) * int64(i.blockSize)
			var err error
			if blockOffset > offset {
				// Partial read at the end
				if len(buffer[blockOffset-offset:]) < i.blockSize {
					blockBuffer := make([]byte, i.blockSize)
					_, err = getData(blockBuffer, blockOffset)
					copy(buffer[blockOffset-offset:], blockBuffer)
				} else {
					// Complete read in the middle
					s := blockOffset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = getData(buffer[s:e], blockOffset)
				}
			} else {
				// Partial read at the start
				blockBuffer := make([]byte, i.blockSize)
				_, err = getData(blockBuffer, blockOffset)
				copy(buffer, blockBuffer[offset-blockOffset:])
			}
			errs <- err
		}(b)
	}

	// Wait for completion, Check for errors and return...
	for b := bStart; b < bEnd; b++ {
		e := <-errs
		if e != nil && !errors.Is(e, io.EOF) {
			return 0, e
		}
	}

	atomic.AddUint64(&i.metricsBlocksRDataBytes, uint64(len(buffer)))

	return len(buffer), nil
}

func (i *S3Storage) WriteAt(buffer []byte, offset int64) (int, error) {
	// Split the read up into blocks, and concurrenty perform the reads...
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	bStart := uint(offset / int64(i.blockSize))
	bEnd := uint((end-1)/uint64(i.blockSize)) + 1

	blocks := bEnd - bStart
	errs := make(chan error, blocks)

	getData := func(buff []byte, off int64) (int, error) {
		if i.dummy {
			atomic.AddUint64(&i.metricsBlocksWPreRCount, 1)
			atomic.AddUint64(&i.metricsBlocksWPreRBytes, uint64(len(buff)))
			return len(buff), nil
		}
		ctx := context.TODO()
		i.lockers[off/int64(i.blockSize)].RLock()
		ctime := time.Now()
		obj, err := i.client.GetObject(ctx, i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
		i.lockers[off/int64(i.blockSize)].RUnlock()
		if err != nil {
			if err.Error() == errNoSuchKey.Error() {
				return len(buff), nil
			}
			return 0, err
		}
		n, err := obj.Read(buff)
		dtime := time.Since(ctime)
		if err == io.EOF {
			atomic.AddUint64(&i.metricsBlocksWPreRCount, 1)
			atomic.AddUint64(&i.metricsBlocksWPreRBytes, uint64(n))
			atomic.AddUint64(&i.metricsBlocksWPreRTimeNS, uint64(dtime.Nanoseconds()))
		} else if err.Error() == errNoSuchKey.Error() {
			return len(buff), nil
		}
		return n, err
	}

	putData := func(buff []byte, off int64) (int, error) {
		if i.dummy {
			atomic.AddUint64(&i.metricsBlocksWCount, 1)
			atomic.AddUint64(&i.metricsBlocksWBytes, uint64(len(buff)))
			return len(buff), nil
		}

		block := off / int64(i.blockSize)
		ctx, cancelFn := context.WithCancel(context.TODO())
		i.lockers[block].Lock()

		i.setContext(int(block), cancelFn)

		ctime := time.Now()
		obj, err := i.client.PutObject(ctx, i.bucket, fmt.Sprintf("%s-%d", i.prefix, off),
			bytes.NewReader(buff), int64(i.blockSize),
			minio.PutObjectOptions{})
		dtime := time.Since(ctime)

		i.setContext(int(block), nil)
		i.lockers[block].Unlock()

		if err == nil {
			atomic.AddUint64(&i.metricsBlocksWCount, 1)
			atomic.AddUint64(&i.metricsBlocksWBytes, uint64(obj.Size))
			atomic.AddUint64(&i.metricsBlocksWTimeNS, uint64(dtime.Nanoseconds()))
		} else {
			return 0, err
		}

		return int(obj.Size), nil
	}

	for b := bStart; b < bEnd; b++ {
		go func(block_no uint) {
			blockOffset := int64(block_no) * int64(i.blockSize)
			var err error
			if blockOffset >= offset {
				// Partial write at the end
				if len(buffer[blockOffset-offset:]) < i.blockSize {
					blockBuffer := make([]byte, i.blockSize)
					// Read existing data
					_, err = getData(blockBuffer, blockOffset)
					if err == nil || errors.Is(err, io.EOF) {
						// Update the data
						copy(blockBuffer, buffer[blockOffset-offset:])
						// Write back
						_, err = putData(blockBuffer, blockOffset)
					}
				} else {
					// Complete write in the middle
					s := blockOffset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = putData(buffer[s:e], blockOffset)
				}
			} else {
				// Partial write at the start
				blockBuffer := make([]byte, i.blockSize)
				_, err = getData(blockBuffer, blockOffset)
				if err == nil || errors.Is(err, io.EOF) {
					copy(blockBuffer[offset-blockOffset:], buffer)
					_, err = putData(blockBuffer, blockOffset)
				}
			}
			errs <- err
		}(b)
	}

	// Wait for completion, Check for errors and return...
	for b := bStart; b < bEnd; b++ {
		e := <-errs
		if e != nil && !errors.Is(e, io.EOF) {
			return 0, e
		}
	}

	atomic.AddUint64(&i.metricsBlocksWDataBytes, uint64(len(buffer)))

	return len(buffer), nil
}

func (i *S3Storage) Flush() error {
	return nil
}

func (i *S3Storage) Size() uint64 {
	return i.size
}

func (i *S3Storage) Close() error {
	return nil
}

func (i *S3Storage) CancelWrites(offset int64, length int64) {
	end := uint64(offset + length)
	if end > i.size {
		end = i.size
	}

	bStart := uint(offset / int64(i.blockSize))
	bEnd := uint((end-1)/uint64(i.blockSize)) + 1

	for b := bStart; b < bEnd; b++ {
		// Cancel any writes for the given block...
		i.setContext(int(b), nil)
	}
}

type S3Metrics struct {
	BlocksWCount     uint64
	BlocksWBytes     uint64
	BlocksWDataBytes uint64
	BlocksWTime      time.Duration
	BlocksWPreRCount uint64
	BlocksWPreRBytes uint64
	BlocksWPreRTime  time.Duration
	BlocksRCount     uint64
	BlocksRBytes     uint64
	BlocksRDataBytes uint64
	BlocksRTime      time.Duration
}

func (i *S3Metrics) String() string {
	return fmt.Sprintf("W %d (%d bytes) (%d dataBytes) %dms Wp %d (%d bytes) %dms R %d (%d bytes) (%d dataBytes) %dms",
		i.BlocksWCount, i.BlocksWBytes, i.BlocksWDataBytes, i.BlocksWTime.Milliseconds(),
		i.BlocksWPreRCount, i.BlocksWPreRBytes, i.BlocksWPreRTime.Milliseconds(),
		i.BlocksRCount, i.BlocksRBytes, i.BlocksRDataBytes, i.BlocksRTime.Milliseconds(),
	)
}

func (i *S3Storage) Metrics() *S3Metrics {
	return &S3Metrics{
		BlocksWCount:     atomic.LoadUint64(&i.metricsBlocksWCount),
		BlocksWBytes:     atomic.LoadUint64(&i.metricsBlocksWBytes),
		BlocksWDataBytes: atomic.LoadUint64(&i.metricsBlocksWDataBytes),
		BlocksWTime:      time.Duration(atomic.LoadUint64(&i.metricsBlocksWTimeNS)),
		BlocksWPreRCount: atomic.LoadUint64(&i.metricsBlocksWPreRCount),
		BlocksWPreRBytes: atomic.LoadUint64(&i.metricsBlocksWPreRBytes),
		BlocksWPreRTime:  time.Duration(atomic.LoadUint64(&i.metricsBlocksWPreRTimeNS)),
		BlocksRCount:     atomic.LoadUint64(&i.metricsBlocksRCount),
		BlocksRBytes:     atomic.LoadUint64(&i.metricsBlocksRBytes),
		BlocksRDataBytes: atomic.LoadUint64(&i.metricsBlocksRDataBytes),
		BlocksRTime:      time.Duration(atomic.LoadUint64(&i.metricsBlocksRTimeNS)),
	}
}
