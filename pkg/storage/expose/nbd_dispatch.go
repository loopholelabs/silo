package expose

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

// TODO: Context, and handle fatal errors

const READ_POOL_BUFFER_SIZE = 256 * 1024
const READ_POOL_SIZE = 128

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
	ASYNC_READS        bool
	ASYNC_WRITES       bool
	fp                 io.ReadWriteCloser
	response_header    []byte
	write_lock         sync.Mutex
	prov               storage.StorageProvider
	fatal              chan error
	pending_responses  sync.WaitGroup
	metric_packets_in  uint64
	metric_packets_out uint64
	read_buffers       chan []byte
}

func NewDispatch(fp io.ReadWriteCloser, prov storage.StorageProvider) *Dispatch {
	d := &Dispatch{
		ASYNC_WRITES:    true,
		ASYNC_READS:     true,
		response_header: make([]byte, 16),
		fatal:           make(chan error, 8),
		fp:              fp,
		prov:            prov,
	}
	d.read_buffers = make(chan []byte, READ_POOL_SIZE)
	for i := 0; i < READ_POOL_SIZE; i++ {
		d.read_buffers <- make([]byte, READ_POOL_BUFFER_SIZE)
	}

	binary.BigEndian.PutUint32(d.response_header, NBD_RESPONSE_MAGIC)
	return d
}

func (d *Dispatch) Wait() {
	// Wait for any pending responses
	d.pending_responses.Wait()
}

/**
 * Write a response...
 *
 */
func (d *Dispatch) writeResponse(respError uint32, respHandle uint64, chunk []byte) error {
	d.write_lock.Lock()
	defer d.write_lock.Unlock()

	//	fmt.Printf("WriteResponse %v %x -> %d\n", d.fp, respHandle, len(chunk))

	binary.BigEndian.PutUint32(d.response_header[4:], respError)
	binary.BigEndian.PutUint64(d.response_header[8:], respHandle)

	_, err := d.fp.Write(d.response_header)
	if err != nil {
		return err
	}
	if len(chunk) > 0 {
		_, err = d.fp.Write(chunk)
		if err != nil {
			return err
		}
	}

	d.metric_packets_out++
	return nil
}

/**
 * This dispatches incoming NBD requests sequentially to the provider.
 *
 */
func (d *Dispatch) Handle() error {
	//	defer func() {
	//		fmt.Printf("Handle %d in %d out\n", d.packets_in, d.packets_out)
	//	}()
	// Speed read and dispatch...

	BUFFER_SIZE := 4 * 1024 * 1024
	buffer := make([]byte, BUFFER_SIZE)
	wp := 0

	request := Request{}

	for {
		//		fmt.Printf("Read from [%d in, %d out] %v\n", d.packets_in, d.packets_out, d.fp)
		n, err := d.fp.Read(buffer[wp:])
		if err != nil {
			return err
		}
		wp += n

		// Now go through processing complete packets
		rp := 0
		for {
			//			fmt.Printf("Processing data %d %d\n", rp, wp)
			// Make sure we have a complete header
			if wp-rp >= 28 {
				// We can read the neader...

				header := buffer[rp : rp+28]
				request.Magic = binary.BigEndian.Uint32(header)
				request.Type = binary.BigEndian.Uint32(header[4:8])
				request.Handle = binary.BigEndian.Uint64(header[8:16])
				request.From = binary.BigEndian.Uint64(header[16:24])
				request.Length = binary.BigEndian.Uint32(header[24:28])

				//fmt.Printf("REQ %v %v\n", d.fp, request)

				if request.Magic != NBD_REQUEST_MAGIC {
					return fmt.Errorf("received invalid MAGIC")
				}

				if request.Type == NBD_CMD_DISCONNECT {
					//					fmt.Printf(" -> CMD_DISCONNECT\n")
					return nil // All done
				} else if request.Type == NBD_CMD_FLUSH {
					return fmt.Errorf("not supported: Flush")
				} else if request.Type == NBD_CMD_READ {
					//					fmt.Printf("READ %x %d\n", request.Handle, request.Length)
					rp += 28
					d.metric_packets_in++
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
					d.metric_packets_in++
					data := make([]byte, request.Length)
					copy(data, buffer[rp:rp+int(request.Length)])
					rp += int(request.Length)
					//					fmt.Printf("WRITE %x %d\n", request.Handle, request.Length)
					err := d.cmdWrite(request.Handle, request.From, request.Length, data)
					if err != nil {
						return err
					}
				} else if request.Type == NBD_CMD_TRIM {
					//					fmt.Printf("TRIM\n")
					rp += 28
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
			//			fmt.Printf("Copy partial %d %d\n", rp, wp)

			copy(buffer, buffer[rp:wp])
		}
		wp -= rp

	}
}

/**
 * cmdRead
 *
 */
func (d *Dispatch) cmdRead(cmd_handle uint64, cmd_from uint64, cmd_length uint32) error {

	performRead := func(handle uint64, from uint64, length uint32) error {
		var b []byte
		var from_pool = false
		if length <= READ_POOL_BUFFER_SIZE {
			// Try to get a buffer from pool
			select {
			case b = <-d.read_buffers:
				from_pool = true
				b = b[:length]
				break
			default:
				break
			}
		}

		// Couldn't get one from the pool
		if b == nil {
			// We'll have to alloc it
			//			fmt.Printf("Alloc %d\n", length)
			b = make([]byte, length)
		}

		data := b // make([]byte, length)
		_, e := d.prov.ReadAt(data, int64(from))
		errorValue := uint32(0)
		if e != nil {
			errorValue = 1
			data = make([]byte, 0) // If there was an error, don't send data
		}
		err := d.writeResponse(errorValue, handle, data)
		// Return it to pool if need to
		if from_pool {
			d.read_buffers <- b[:READ_POOL_BUFFER_SIZE]
		}
		return err
	}

	if d.ASYNC_READS {
		d.pending_responses.Add(1)
		go func() {
			err := performRead(cmd_handle, cmd_from, cmd_length)
			if err != nil {
				d.fatal <- err
			}
			d.pending_responses.Done()
		}()
	} else {
		return performRead(cmd_handle, cmd_from, cmd_length)
	}
	return nil
}

/**
 * cmdWrite
 *
 */
func (d *Dispatch) cmdWrite(cmd_handle uint64, cmd_from uint64, cmd_length uint32, cmd_data []byte) error {
	performWrite := func(handle uint64, from uint64, length uint32, data []byte) error {
		_, e := d.prov.WriteAt(data, int64(from))
		errorValue := uint32(0)
		if e != nil {
			errorValue = 1
		}
		return d.writeResponse(errorValue, handle, []byte{})
	}

	if d.ASYNC_WRITES {
		d.pending_responses.Add(1)
		go func() {
			err := performWrite(cmd_handle, cmd_from, cmd_length, cmd_data)
			if err != nil {
				d.fatal <- err
			}
			d.pending_responses.Done()
		}()
	} else {
		return performWrite(cmd_handle, cmd_from, cmd_length, cmd_data)
	}
	return nil
}

/**
 * cmdTrim
 *
 */
func (d *Dispatch) cmdTrim(handle uint64, from uint64, length uint32) error {
	// Ask the provider
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
