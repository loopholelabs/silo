package sources

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/minio/minio-go"
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
}

func NewS3Storage(endpoint string,
	access string,
	secretAccess string,
	bucket string,
	prefix string,
	size uint64,
	blockSize int) (*S3Storage, error) {
	client, err := minio.New(endpoint, access, secretAccess, true)
	if err != nil {
		return nil, err
	}

	return &S3Storage{
		size:      size,
		blockSize: blockSize,
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
	}, nil
}

func NewS3StorageCreate(endpoint string,
	access string,
	secretAccess string,
	bucket string,
	prefix string,
	size uint64,
	blockSize int) (*S3Storage, error) {
	client, err := minio.New(endpoint, access, secretAccess, true)
	if err != nil {
		return nil, err
	}

	b_end := (int(size) + blockSize - 1) / blockSize
	buffer := make([]byte, blockSize)

	for b := 0; b < b_end; b++ {
		offset := b * blockSize

		_, err := client.PutObject(bucket, fmt.Sprintf("%s-%d", prefix, offset), bytes.NewReader(buffer), int64(blockSize), minio.PutObjectOptions{})
		if err != nil {
			return nil, err
		}
	}

	return &S3Storage{
		size:      size,
		blockSize: blockSize,
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
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
		obj, err := i.client.GetObject(i.bucket, fmt.Sprintf("%s-%d", i.prefix, off), minio.GetObjectOptions{})
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
		if e != nil {
			return 0, e
		}
	}

	return len(buffer), nil
}

func (i *S3Storage) WriteAt(buffer []byte, offset int64) (int, error) {
	// TODO...
	return 0, nil
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
