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

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	errNoSuchKey = errors.New("The specified key does not exist.") // Minio doesn't export errors
)

type S3Storage struct {
	client                         *minio.Client
	dummy                          bool
	bucket                         string
	prefix                         string
	size                           uint64
	block_size                     int
	lockers                        []sync.RWMutex
	contexts                       []context.CancelFunc
	contexts_lock                  sync.Mutex
	metrics_blocks_w_count         uint64
	metrics_blocks_w_bytes         uint64
	metrics_blocks_w_time_ns       uint64
	metrics_blocks_w_pre_r_count   uint64
	metrics_blocks_w_pre_r_bytes   uint64
	metrics_blocks_w_pre_r_time_ns uint64
	metrics_blocks_r_count         uint64
	metrics_blocks_r_bytes         uint64
	metrics_blocks_r_time_ns       uint64
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
		size:       size,
		block_size: blockSize,
		client:     client,
		bucket:     bucket,
		prefix:     prefix,
		lockers:    make([]sync.RWMutex, numBlocks),
		contexts:   make([]context.CancelFunc, numBlocks),
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
		size:       size,
		block_size: blockSize,
		client:     client,
		bucket:     bucket,
		prefix:     prefix,
		lockers:    make([]sync.RWMutex, numBlocks),
		contexts:   make([]context.CancelFunc, numBlocks),
	}, nil
}

func (i *S3Storage) setContext(block int, cancel context.CancelFunc) {
	i.contexts_lock.Lock()
	ex := i.contexts[block]
	if ex != nil {
		ex() // Cancel any existing context for this block
	}
	i.contexts[block] = cancel // Set to the new context
	i.contexts_lock.Unlock()
}

func (i *S3Storage) ReadAt(buffer []byte, offset int64) (int, error) {
	// Split the read up into blocks, and concurrenty perform the reads...
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	blocks := b_end - b_start
	errs := make(chan error, blocks)

	getData := func(buff []byte, off int64) (int, error) {
		i.lockers[off/int64(i.block_size)].RLock()
		ctime := time.Now()
		obj, err := i.client.GetObject(context.TODO(), i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
		i.lockers[off/int64(i.block_size)].RUnlock()
		if err != nil {
			if err.Error() == errNoSuchKey.Error() {
				return len(buff), nil
			}
			return 0, err
		}
		n, err := obj.Read(buff)
		dtime := time.Since(ctime)
		if err == nil {
			atomic.AddUint64(&i.metrics_blocks_r_count, 1)
			atomic.AddUint64(&i.metrics_blocks_r_bytes, uint64(n))
			atomic.AddUint64(&i.metrics_blocks_r_time_ns, uint64(dtime.Nanoseconds()))
		} else if err.Error() == errNoSuchKey.Error() {
			return len(buff), nil
		}

		return n, err
	}

	for b := b_start; b < b_end; b++ {
		go func(block_no uint) {
			block_offset := int64(block_no) * int64(i.block_size)
			var err error
			if block_offset > offset {
				// Partial read at the end
				if len(buffer[block_offset-offset:]) < i.block_size {
					block_buffer := make([]byte, i.block_size)
					_, err = getData(block_buffer, block_offset)
					copy(buffer[block_offset-offset:], block_buffer)
				} else {
					// Complete read in the middle
					s := block_offset - offset
					e := s + int64(i.block_size)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = getData(buffer[s:e], block_offset)
				}
			} else {
				// Partial read at the start
				block_buffer := make([]byte, i.block_size)
				_, err = getData(block_buffer, block_offset)
				copy(buffer, block_buffer[offset-block_offset:])
			}
			errs <- err
		}(b)
	}

	// Wait for completion, Check for errors and return...
	for b := b_start; b < b_end; b++ {
		e := <-errs
		if e != nil && !errors.Is(e, io.EOF) {
			return 0, e
		}
	}

	return len(buffer), nil
}

func (i *S3Storage) WriteAt(buffer []byte, offset int64) (int, error) {
	// Split the read up into blocks, and concurrenty perform the reads...
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	blocks := b_end - b_start
	errs := make(chan error, blocks)

	getData := func(buff []byte, off int64) (int, error) {
		ctx := context.TODO()
		i.lockers[off/int64(i.block_size)].RLock()
		ctime := time.Now()
		obj, err := i.client.GetObject(ctx, i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
		i.lockers[off/int64(i.block_size)].RUnlock()
		if err != nil {
			if err.Error() == errNoSuchKey.Error() {
				return len(buff), nil
			}
			return 0, err
		}
		n, err := obj.Read(buff)
		dtime := time.Since(ctime)
		if err == nil {
			atomic.AddUint64(&i.metrics_blocks_w_pre_r_count, 1)
			atomic.AddUint64(&i.metrics_blocks_w_pre_r_bytes, uint64(n))
			atomic.AddUint64(&i.metrics_blocks_w_pre_r_time_ns, uint64(dtime.Nanoseconds()))
		} else if err.Error() == errNoSuchKey.Error() {
			return len(buff), nil
		}
		return n, err
	}

	putData := func(buff []byte, off int64) (int, error) {
		block := off / int64(i.block_size)
		ctx, cancelFn := context.WithCancel(context.TODO())
		i.lockers[block].Lock()

		i.setContext(int(block), cancelFn)

		ctime := time.Now()
		obj, err := i.client.PutObject(ctx, i.bucket, fmt.Sprintf("%s-%d", i.prefix, off),
			bytes.NewReader(buff), int64(i.block_size),
			minio.PutObjectOptions{})
		dtime := time.Since(ctime)

		i.setContext(int(block), nil)
		i.lockers[block].Unlock()

		if err == nil {
			atomic.AddUint64(&i.metrics_blocks_w_count, 1)
			atomic.AddUint64(&i.metrics_blocks_w_bytes, uint64(obj.Size))
			atomic.AddUint64(&i.metrics_blocks_w_time_ns, uint64(dtime.Nanoseconds()))
		} else {
			// Currently, if the context was canceled, we ignore it.
			if !errors.Is(err, context.Canceled) {
				return 0, err
			}
		}

		return int(obj.Size), nil
	}

	for b := b_start; b < b_end; b++ {
		go func(block_no uint) {
			block_offset := int64(block_no) * int64(i.block_size)
			var err error
			if block_offset > offset {
				// Partial write at the end
				if len(buffer[block_offset-offset:]) < i.block_size {
					block_buffer := make([]byte, i.block_size)
					// Read existing data
					_, err = getData(block_buffer, block_offset)
					if err == nil || errors.Is(err, io.EOF) {
						// Update the data
						copy(block_buffer, buffer[block_offset-offset:])
						// Write back
						_, err = putData(block_buffer, block_offset)
					}
				} else {
					// Complete write in the middle
					s := block_offset - offset
					e := s + int64(i.block_size)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = putData(buffer[s:e], block_offset)
				}
			} else {
				// Partial write at the start
				block_buffer := make([]byte, i.block_size)
				_, err = getData(block_buffer, block_offset)
				if err == nil || errors.Is(err, io.EOF) {
					copy(block_buffer[offset-block_offset:], buffer)
					_, err = putData(block_buffer, block_offset)
				}
			}
			errs <- err
		}(b)
	}

	// Wait for completion, Check for errors and return...
	for b := b_start; b < b_end; b++ {
		e := <-errs
		if e != nil && !errors.Is(e, io.EOF) {
			return 0, e
		}
	}

	return len(buffer), nil
}

func (i *S3Storage) Flush() error {
	return nil
}

func (i *S3Storage) Size() uint64 {
	return uint64(i.size)
}

func (i *S3Storage) Close() error {
	return nil
}

func (i *S3Storage) CancelWrites(offset int64, length int64) {
	end := uint64(offset + length)
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.block_size))
	b_end := uint((end-1)/uint64(i.block_size)) + 1

	for b := b_start; b < b_end; b++ {
		// Cancel any writes for the given block...
		i.setContext(int(b), nil)
	}
}

type S3Metrics struct {
	Blocks_w_count       uint64
	Blocks_w_bytes       uint64
	Blocks_w_time        time.Duration
	Blocks_w_pre_r_count uint64
	Blocks_w_pre_r_bytes uint64
	Blocks_w_pre_r_time  time.Duration
	Blocks_r_count       uint64
	Blocks_r_bytes       uint64
	Blocks_r_time        time.Duration
}

func (i *S3Storage) Metrics() *S3Metrics {
	return &S3Metrics{
		Blocks_w_count:       atomic.LoadUint64(&i.metrics_blocks_w_count),
		Blocks_w_bytes:       atomic.LoadUint64(&i.metrics_blocks_w_bytes),
		Blocks_w_time:        time.Duration(atomic.LoadUint64(&i.metrics_blocks_w_time_ns)),
		Blocks_w_pre_r_count: atomic.LoadUint64(&i.metrics_blocks_w_pre_r_count),
		Blocks_w_pre_r_bytes: atomic.LoadUint64(&i.metrics_blocks_w_pre_r_bytes),
		Blocks_w_pre_r_time:  time.Duration(atomic.LoadUint64(&i.metrics_blocks_w_pre_r_time_ns)),
		Blocks_r_count:       atomic.LoadUint64(&i.metrics_blocks_r_count),
		Blocks_r_bytes:       atomic.LoadUint64(&i.metrics_blocks_r_bytes),
		Blocks_r_time:        time.Duration(atomic.LoadUint64(&i.metrics_blocks_r_time_ns)),
	}
}
