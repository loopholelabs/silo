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
	client    *minio.Client
	bucket    string
	prefix    string
	size      uint64
	blockSize int
	lockers   []sync.RWMutex
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
		size:      size,
		blockSize: blockSize,
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
		lockers:   make([]sync.RWMutex, numBlocks),
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

	b_end := (int(size) + blockSize - 1) / blockSize
	buffer := make([]byte, blockSize)

	for b := 0; b < b_end; b++ {
		offset := b * blockSize

		_, err := client.PutObject(context.TODO(), bucket, fmt.Sprintf("%s-%d", prefix, offset), bytes.NewReader(buffer), int64(blockSize), minio.PutObjectOptions{})
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
	}, nil
}

func (i *S3Storage) ReadAt(buffer []byte, offset int64) (int, error) {
	// Split the read up into blocks, and concurrenty perform the reads...
	end := uint64(offset + int64(len(buffer)))
	if end > i.size {
		end = i.size
	}

	b_start := uint(offset / int64(i.blockSize))
	b_end := uint((end-1)/uint64(i.blockSize)) + 1

	blocks := b_end - b_start
	errs := make(chan error, blocks)

	getData := func(buff []byte, off int64) (int, error) {
		i.lockers[off/int64(i.blockSize)].RLock()
		obj, err := i.client.GetObject(context.TODO(), i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
		i.lockers[off/int64(i.blockSize)].RUnlock()
		if err != nil {
			return 0, err
		}
		return obj.Read(buff)
	}

	for b := b_start; b < b_end; b++ {
		go func(block_no uint) {
			block_offset := int64(block_no) * int64(i.blockSize)
			var err error
			if block_offset > offset {
				// Partial read at the end
				if len(buffer[block_offset-offset:]) < i.blockSize {
					block_buffer := make([]byte, i.blockSize)
					_, err = getData(block_buffer, block_offset)
					copy(buffer[block_offset-offset:], block_buffer)
				} else {
					// Complete read in the middle
					s := block_offset - offset
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = getData(buffer[s:e], block_offset)
				}
			} else {
				// Partial read at the start
				block_buffer := make([]byte, i.blockSize)
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

	b_start := uint(offset / int64(i.blockSize))
	b_end := uint((end-1)/uint64(i.blockSize)) + 1

	blocks := b_end - b_start
	errs := make(chan error, blocks)

	getData := func(buff []byte, off int64) (int, error) {
		i.lockers[off/int64(i.blockSize)].RLock()
		obj, err := i.client.GetObject(context.TODO(), i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
		i.lockers[off/int64(i.blockSize)].RUnlock()
		if err != nil {
			return 0, err
		}
		return obj.Read(buff)
	}

	putData := func(buff []byte, off int64) (int, error) {
		i.lockers[off/int64(i.blockSize)].Lock()
		obj, err := i.client.PutObject(context.TODO(), i.bucket, fmt.Sprintf("%s-%d", i.prefix, off),
			bytes.NewReader(buff), int64(i.blockSize),
			minio.PutObjectOptions{})
		i.lockers[off/int64(i.blockSize)].Unlock()
		if err != nil {
			return 0, err
		}
		return int(obj.Size), nil
	}

	for b := b_start; b < b_end; b++ {
		go func(block_no uint) {
			block_offset := int64(block_no) * int64(i.blockSize)
			var err error
			if block_offset > offset {
				// Partial write at the end
				if len(buffer[block_offset-offset:]) < i.blockSize {
					block_buffer := make([]byte, i.blockSize)
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
					e := s + int64(i.blockSize)
					if e > int64(len(buffer)) {
						e = int64(len(buffer))
					}
					_, err = putData(buffer[s:e], block_offset)
				}
			} else {
				// Partial write at the start
				block_buffer := make([]byte, i.blockSize)
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