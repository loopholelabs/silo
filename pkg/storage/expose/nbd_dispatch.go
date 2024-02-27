package expose

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
)

// TODO: Context, and handle fatal errors

type Dispatch struct {
	ASYNC_READS      bool
	ASYNC_WRITES     bool
	fp               io.ReadWriteCloser
	responseHeader   []byte
	writeLock        sync.Mutex
	prov             storage.StorageProvider
	fatal            chan error
	pendingResponses sync.WaitGroup
	packets_in       uint64
	packets_out      uint64
}

func NewDispatch(fp io.ReadWriteCloser, prov storage.StorageProvider) *Dispatch {
	d := &Dispatch{
		ASYNC_WRITES:   true,
		ASYNC_READS:    true,
		responseHeader: make([]byte, 16),
		fatal:          make(chan error, 8),
		fp:             fp,
		prov:           prov,
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

	d.packets_out++
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
					d.packets_in++
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
					d.packets_in++
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
		data := make([]byte, length)
		_, e := d.prov.ReadAt(data, int64(from))
		errorValue := uint32(0)
		if e != nil {
			errorValue = 1
			data = make([]byte, 0) // If there was an error, don't send data
		}
		return d.writeResponse(errorValue, handle, data)
	}

	if d.ASYNC_READS {
		d.pendingResponses.Add(1)
		go func() {
			err := performRead(cmd_handle, cmd_from, cmd_length)
			if err != nil {
				d.fatal <- err
			}
			d.pendingResponses.Done()
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
		d.pendingResponses.Add(1)
		go func() {
			err := performWrite(cmd_handle, cmd_from, cmd_length, cmd_data)
			if err != nil {
				d.fatal <- err
			}
			d.pendingResponses.Done()
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
