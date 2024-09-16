package sources

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	errNoSuchKey = errors.New("The specified key does not exist.") // Minio doesn't export errors
)

type S3Storage struct {
	client        *minio.Client
	bucket        string
	prefix        string
	size          uint64
	block_size    int
	lockers       []sync.RWMutex
	contexts      []context.CancelFunc
	contexts_lock sync.Mutex
}

func NewS3Storage(endpoint string,
	access string,
	secretAccess string,
	bucket string,
	prefix string,
	size uint64,
	blockSize int) (*S3Storage, error) {

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secretAccess, ""),
		Secure: false,
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

func NewS3StorageCreate(endpoint string,
	access string,
	secretAccess string,
	bucket string,
	prefix string,
	size uint64,
	blockSize int) (*S3Storage, error) {

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secretAccess, ""),
		Secure: false,
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

	/*
		b_end := (int(size) + blockSize - 1) / blockSize
		buffer := make([]byte, blockSize)

		// TODO: Do these concurrently, or switch to lazy create on read/write.
		for b := 0; b < b_end; b++ {
			offset := b * blockSize

			_, err := client.PutObject(context.TODO(), bucket, fmt.Sprintf("%s-%d", prefix, offset), bytes.NewReader(buffer), int64(blockSize), minio.PutObjectOptions{})
			if err != nil {
				return nil, err
			}
		}
	*/

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
		obj, err := i.client.GetObject(context.TODO(), i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
		i.lockers[off/int64(i.block_size)].RUnlock()
		if err != nil {
			if err.Error() == errNoSuchKey.Error() {
				return len(buff), nil
			}
			return 0, err
		}
		n, err := obj.Read(buff)
		if err.Error() == errNoSuchKey.Error() {
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
		obj, err := i.client.GetObject(ctx, i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
		i.lockers[off/int64(i.block_size)].RUnlock()
		if err != nil {
			if err.Error() == errNoSuchKey.Error() {
				return len(buff), nil
			}
			return 0, err
		}
		n, err := obj.Read(buff)
		if err.Error() == errNoSuchKey.Error() {
			return len(buff), nil
		}
		return n, err
	}

	putData := func(buff []byte, off int64) (int, error) {
		block := off / int64(i.block_size)
		ctx, cancelFn := context.WithCancel(context.TODO())
		i.lockers[block].Lock()

		i.setContext(int(block), cancelFn)

		obj, err := i.client.PutObject(ctx, i.bucket, fmt.Sprintf("%s-%d", i.prefix, off),
			bytes.NewReader(buff), int64(i.block_size),
			minio.PutObjectOptions{})

		i.setContext(int(block), nil)
		i.lockers[block].Unlock()
		if err != nil {
			return 0, err
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
