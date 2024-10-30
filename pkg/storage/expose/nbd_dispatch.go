package expose

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

const NBDDISPATCH_BUFFER_SIZE = 4 * 1024 * 1024

/**
 * Exposes a storage provider as an nbd device
 *
 */
const NBD_COMMAND = 0xab00
const NBD_SET_SOCK = 0 | NBD_COMMAND
const NBD_SET_BLKSIZE = 1 | NBD_COMMAND
const NBD_SET_SIZE = 2 | NBD_COMMAND
const NBD_DO_IT = 3 | NBD_COMMAND
const NBD_CLEAR_SOCK = 4 | NBD_COMMAND
const NBD_CLEAR_QUE = 5 | NBD_COMMAND
const NBD_PRINT_DEBUG = 6 | NBD_COMMAND
const NBD_SET_SIZE_BLOCKS = 7 | NBD_COMMAND
const NBD_DISCONNECT = 8 | NBD_COMMAND
const NBD_SET_TIMEOUT = 9 | NBD_COMMAND
const NBD_SET_FLAGS = 10 | NBD_COMMAND

// NBD Commands
const NBD_CMD_READ = 0
const NBD_CMD_WRITE = 1
const NBD_CMD_DISCONNECT = 2
const NBD_CMD_FLUSH = 3
const NBD_CMD_TRIM = 4

// NBD Flags
const NBD_FLAG_HAS_FLAGS = (1 << 0)
const NBD_FLAG_READ_ONLY = (1 << 1)
const NBD_FLAG_SEND_FLUSH = (1 << 2)
const NBD_FLAG_SEND_TRIM = (1 << 5)

const NBD_REQUEST_MAGIC = 0x25609513
const NBD_RESPONSE_MAGIC = 0x67446698

// NBD Request packet
type Request struct {
	Magic  uint32
	Type   uint32
	Handle uint64
	From   uint64
	Length uint32
}

// NBD Response packet
type Response struct {
	Magic  uint32
	Error  uint32
	Handle uint64
}

type Dispatch struct {
	ctx              context.Context
	asyncReads       bool
	asyncWrites      bool
	fp               io.ReadWriteCloser
	responseHeader   []byte
	writeLock        sync.Mutex
	prov             storage.StorageProvider
	fatal            chan error
	pendingResponses sync.WaitGroup
	metricPacketsIn  uint64
	metricPacketsOut uint64
}

func NewDispatch(ctx context.Context, fp io.ReadWriteCloser, prov storage.StorageProvider) *Dispatch {

	d := &Dispatch{
		asyncWrites:    true,
		asyncReads:     true,
		responseHeader: make([]byte, 16),
		fatal:          make(chan error, 8),
		fp:             fp,
		prov:           prov,
		ctx:            ctx,
	}

	binary.BigEndian.PutUint32(d.responseHeader, NBD_RESPONSE_MAGIC)
	return d
}

func (d *Dispatch) Wait() {
	// Wait for any pending responses
	d.pendingResponses.Wait()
}

/**
 * Write a response...
 *
 */
func (d *Dispatch) writeResponse(respError uint32, respHandle uint64, chunk []byte) error {
	d.writeLock.Lock()
	defer d.writeLock.Unlock()

	//	fmt.Printf("WriteResponse %v %x -> %d\n", d.fp, respHandle, len(chunk))

	binary.BigEndian.PutUint32(d.responseHeader[4:], respError)
	binary.BigEndian.PutUint64(d.responseHeader[8:], respHandle)

	_, err := d.fp.Write(d.responseHeader)
	if err != nil {
		return err
	}
	if len(chunk) > 0 {
		_, err = d.fp.Write(chunk)
		if err != nil {
			return err
		}
	}

	d.metricPacketsOut++
	return nil
}

/**
 * This dispatches incoming NBD requests sequentially to the provider.
 *
 */
func (d *Dispatch) Handle() error {
	buffer := make([]byte, NBDDISPATCH_BUFFER_SIZE)
	wp := 0

	request := Request{}

	for {
		n, err := d.fp.Read(buffer[wp:])
		if err != nil {
			return err
		}
		wp += n

		// Now go through processing complete packets
		rp := 0
		for {

			// If the context has been cancelled, quit
			select {
			case <-d.ctx.Done():
				return d.ctx.Err()
			default:
			}

			// Make sure we have a complete header
			if wp-rp >= 28 {
				// We can read the neader...

				header := buffer[rp : rp+28]
				request.Magic = binary.BigEndian.Uint32(header)
				request.Type = binary.BigEndian.Uint32(header[4:8])
				request.Handle = binary.BigEndian.Uint64(header[8:16])
				request.From = binary.BigEndian.Uint64(header[16:24])
				request.Length = binary.BigEndian.Uint32(header[24:28])

				if request.Magic != NBD_REQUEST_MAGIC {
					return fmt.Errorf("received invalid MAGIC")
				}

				if request.Type == NBD_CMD_DISCONNECT {
					return nil // All done
				} else if request.Type == NBD_CMD_FLUSH {
					return fmt.Errorf("not supported: Flush")
				} else if request.Type == NBD_CMD_READ {
					rp += 28
					d.metricPacketsIn++
					err := d.cmdRead(request.Handle, request.From, request.Length)
					if err != nil {
						return err
					}
				} else if request.Type == NBD_CMD_WRITE {
					rp += 28
					if wp-rp < int(request.Length) {
						rp -= 28
						break // We don't have enough data yet... Wait for next read
					}
					d.metricPacketsIn++
					data := make([]byte, request.Length)
					copy(data, buffer[rp:rp+int(request.Length)])
					rp += int(request.Length)
					err := d.cmdWrite(request.Handle, request.From, request.Length, data)
					if err != nil {
						return err
					}
				} else if request.Type == NBD_CMD_TRIM {
					rp += 28
					d.metricPacketsIn++
					err = d.cmdTrim(request.Handle, request.From, request.Length)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("nbd not implemented %d", request.Type)
				}

			} else {
				break // Try again when we have more data...
			}
		}
		// Now we need to move any partial to the start
		if rp != 0 && rp != wp {
			copy(buffer, buffer[rp:wp])
		}
		wp -= rp
	}
}

/**
 * cmdRead
 *
 */
func (d *Dispatch) cmdRead(cmdHandle uint64, cmdFrom uint64, cmdLength uint32) error {

	performRead := func(handle uint64, from uint64, length uint32) error {
		errchan := make(chan error)
		data := make([]byte, length)

		go func() {
			_, e := d.prov.ReadAt(data, int64(from))
			errchan <- e
		}()

		// Wait until either the ReadAt completed, or our context is cancelled...
		var e error
		select {
		case <-d.ctx.Done():
			e = d.ctx.Err()
		case e = <-errchan:
		}

		errorValue := uint32(0)
		if e != nil {
			errorValue = 1
			data = make([]byte, 0) // If there was an error, don't send data
		}
		return d.writeResponse(errorValue, handle, data)
	}

	if d.asyncReads {
		d.pendingResponses.Add(1)
		go func() {
			err := performRead(cmdHandle, cmdFrom, cmdLength)
			if err != nil {
				d.fatal <- err
			}
			d.pendingResponses.Done()
		}()
	} else {
		d.pendingResponses.Add(1)
		err := performRead(cmdHandle, cmdFrom, cmdLength)
		d.pendingResponses.Done()
		return err
	}
	return nil
}

/**
 * cmdWrite
 *
 */
func (d *Dispatch) cmdWrite(cmdHandle uint64, cmdFrom uint64, cmdLength uint32, cmdData []byte) error {
	performWrite := func(handle uint64, from uint64, _ uint32, data []byte) error {
		errchan := make(chan error)
		go func() {
			_, e := d.prov.WriteAt(data, int64(from))
			errchan <- e
		}()

		// Wait until either the WriteAt completed, or our context is cancelled...
		var e error
		select {
		case <-d.ctx.Done():
			e = d.ctx.Err()
		case e = <-errchan:
		}

		errorValue := uint32(0)
		if e != nil {
			errorValue = 1
		}
		return d.writeResponse(errorValue, handle, []byte{})
	}

	if d.asyncWrites {
		d.pendingResponses.Add(1)
		go func() {
			err := performWrite(cmdHandle, cmdFrom, cmdLength, cmdData)
			if err != nil {
				d.fatal <- err
			}
			d.pendingResponses.Done()
		}()
	} else {
		d.pendingResponses.Add(1)
		err := performWrite(cmdHandle, cmdFrom, cmdLength, cmdData)
		d.pendingResponses.Done()
		return err
	}
	return nil
}

/**
 * cmdTrim
 *
 */
func (d *Dispatch) cmdTrim(handle uint64, from uint64, length uint32) error {
	// TODO: Ask the provider
	/*
		e := d.prov.Trim(from, length)
		if e != storage.StorageError_SUCCESS {
			err := d.writeResponse(1, handle, []byte{})
			if err != nil {
				return err
			}
		} else {
	*/
	err := d.writeResponse(0, handle, []byte{})
	if err != nil {
		return err
	}
	//	}
	return nil
}
