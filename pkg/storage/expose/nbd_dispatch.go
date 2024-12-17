package expose

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
)

var ErrShuttingDown = errors.New("shutting down. Cannot serve any new requests")

const dispatchBufferSize = 4 * 1024 * 1024

/**
 * Exposes a storage provider as an nbd device
 *
 */
const NBDCommand = 0xab00
const NBDSetSock = 0 | NBDCommand
const NBDSetBlksize = 1 | NBDCommand
const NBDSetSize = 2 | NBDCommand
const NBDDoIt = 3 | NBDCommand
const NBDClearSock = 4 | NBDCommand
const NBDClearQue = 5 | NBDCommand
const NBDPrintDebug = 6 | NBDCommand
const NBDSetSizeBlocks = 7 | NBDCommand
const NBDDisconnect = 8 | NBDCommand
const NBDSetTimeout = 9 | NBDCommand
const NBDSetFlags = 10 | NBDCommand

// NBD Commands
const NBDCmdRead = 0
const NBDCmdWrite = 1
const NBDCmdDisconnect = 2
const NBDCmdFlush = 3
const NBDCmdTrim = 4

// NBD Flags
const NBDFlagHasFlags = (1 << 0)
const NBDFlagReadOnly = (1 << 1)
const NBDFlagSendFlush = (1 << 2)
const NBDFlagSendTrim = (1 << 5)

const NBDRequestMagic = 0x25609513
const NBDResponseMagic = 0x67446698

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
	logger             types.Logger
	dev                string
	ctx                context.Context
	asyncReads         bool
	asyncWrites        bool
	fp                 io.ReadWriteCloser
	responseHeader     []byte
	writeLock          sync.Mutex
	prov               storage.Provider
	fatal              chan error
	pendingResponses   sync.WaitGroup
	shuttingDown       bool
	shuttingDownLock   sync.Mutex
	metricPacketsIn    uint64
	metricPacketsOut   uint64
	metricReadAt       uint64
	metricReadAtBytes  uint64
	metricReadAtTime   uint64
	metricWriteAt      uint64
	metricWriteAtBytes uint64
	metricWriteAtTime  uint64
}

type DispatchMetrics struct {
	PacketsIn    uint64
	PacketsOut   uint64
	ReadAt       uint64
	ReadAtBytes  uint64
	ReadAtTime   time.Duration
	WriteAt      uint64
	WriteAtBytes uint64
	WriteAtTime  time.Duration
}

func (dm *DispatchMetrics) Add(delta *DispatchMetrics) {
	dm.PacketsIn += delta.PacketsIn
	dm.PacketsOut += delta.PacketsOut
	dm.ReadAt += delta.ReadAt
	dm.ReadAtBytes += delta.ReadAtBytes
	dm.ReadAtTime += delta.ReadAtTime
	dm.WriteAt += delta.WriteAt
	dm.WriteAtBytes += delta.WriteAtBytes
	dm.WriteAtTime += delta.WriteAtTime
}

func NewDispatch(ctx context.Context, name string, logger types.Logger, fp io.ReadWriteCloser, prov storage.Provider) *Dispatch {

	d := &Dispatch{
		logger:         logger,
		dev:            name,
		asyncWrites:    true,
		asyncReads:     true,
		responseHeader: make([]byte, 16),
		fatal:          make(chan error, 1),
		fp:             fp,
		prov:           prov,
		ctx:            ctx,
	}

	binary.BigEndian.PutUint32(d.responseHeader, NBDResponseMagic)
	return d
}

func (d *Dispatch) GetMetrics() *DispatchMetrics {
	return &DispatchMetrics{
		PacketsIn:    atomic.LoadUint64(&d.metricPacketsIn),
		PacketsOut:   atomic.LoadUint64(&d.metricPacketsOut),
		ReadAt:       atomic.LoadUint64(&d.metricReadAt),
		ReadAtBytes:  atomic.LoadUint64(&d.metricReadAtBytes),
		ReadAtTime:   time.Duration(atomic.LoadUint64(&d.metricReadAtTime)),
		WriteAt:      atomic.LoadUint64(&d.metricWriteAt),
		WriteAtBytes: atomic.LoadUint64(&d.metricWriteAtBytes),
		WriteAtTime:  time.Duration(atomic.LoadUint64(&d.metricWriteAtTime)),
	}
}

func (d *Dispatch) Wait() {
	d.shuttingDownLock.Lock()
	d.shuttingDown = true
	defer d.shuttingDownLock.Unlock()
	// Stop accepting any new requests...

	if d.logger != nil {
		d.logger.Trace().Str("device", d.dev).Msg("nbd waiting for pending responses")
	}
	// Wait for any pending responses
	d.pendingResponses.Wait()
	if d.logger != nil {
		d.logger.Trace().Str("device", d.dev).Msg("nbd all responses sent")
	}
}

/**
 * Write a response...
 *
 */
func (d *Dispatch) writeResponse(respError uint32, respHandle uint64, chunk []byte) error {
	d.writeLock.Lock()
	defer d.writeLock.Unlock()

	if d.logger != nil {
		d.logger.Trace().
			Str("device", d.dev).
			Uint32("respError", respError).
			Uint64("respHandle", respHandle).
			Int("data", len(chunk)).
			Msg("nbd writing response")
	}

	binary.BigEndian.PutUint32(d.responseHeader[4:], respError)
	binary.BigEndian.PutUint64(d.responseHeader[8:], respHandle)

	_, err := d.fp.Write(d.responseHeader)
	if err != nil {
		if d.logger != nil {
			d.logger.Trace().
				Str("device", d.dev).
				Uint32("respError", respError).
				Uint64("respHandle", respHandle).
				Int("data", len(chunk)).
				Err(err).
				Msg("nbd error writing response header")
		}
		return err
	}
	if len(chunk) > 0 {
		_, err = d.fp.Write(chunk)
		if err != nil {
			if d.logger != nil {
				d.logger.Trace().
					Str("device", d.dev).
					Uint32("respError", respError).
					Uint64("respHandle", respHandle).
					Int("data", len(chunk)).
					Err(err).
					Msg("nbd error writing response data")
			}
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
	buffer := make([]byte, dispatchBufferSize)
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
	process:
		for {

			// If the context has been cancelled, quit
			select {

			// Check if there is a fatal error from an async read/write to return
			case err := <-d.fatal:
				return err

			case <-d.ctx.Done():
				if d.logger != nil {
					d.logger.Trace().
						Str("device", d.dev).
						Msg("nbd handler context cancelled")
				}
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

				if request.Magic != NBDRequestMagic {
					return fmt.Errorf("received invalid MAGIC")
				}

				switch request.Type {
				case NBDCmdDisconnect:
					if d.logger != nil {
						d.logger.Trace().
							Str("device", d.dev).
							Msg("nbd disconnect received")
					}
					return nil // All done
				case NBDCmdFlush:
					return fmt.Errorf("not supported: Flush")
				case NBDCmdRead:
					rp += 28
					d.metricPacketsIn++
					err := d.cmdRead(request.Handle, request.From, request.Length)
					if err != nil {
						return err
					}
				case NBDCmdWrite:
					rp += 28
					if wp-rp < int(request.Length) {
						rp -= 28
						break process // We don't have enough data yet... Wait for next read
					}
					d.metricPacketsIn++
					data := make([]byte, request.Length)
					copy(data, buffer[rp:rp+int(request.Length)])
					rp += int(request.Length)
					err := d.cmdWrite(request.Handle, request.From, request.Length, data)
					if err != nil {
						return err
					}
				case NBDCmdTrim:
					rp += 28
					d.metricPacketsIn++
					err = d.cmdTrim(request.Handle, request.From, request.Length)
					if err != nil {
						return err
					}
				default:
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
	if d.logger != nil {
		d.logger.Trace().
			Str("device", d.dev).
			Uint64("cmdHandle", cmdHandle).
			Uint64("cmdFrom", cmdFrom).
			Uint32("cmdLength", cmdLength).
			Msg("nbd cmdRead")
	}

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

		if e != nil {
			return d.writeResponse(1, handle, []byte{})
		}
		return d.writeResponse(0, handle, data)
	}

	d.shuttingDownLock.Lock()
	if !d.shuttingDown {
		d.pendingResponses.Add(1)
	} else {
		d.shuttingDownLock.Unlock()
		return ErrShuttingDown
	}
	d.shuttingDownLock.Unlock()

	if d.asyncReads {
		go func() {
			ctime := time.Now()
			err := performRead(cmdHandle, cmdFrom, cmdLength)
			if err == nil {
				atomic.AddUint64(&d.metricReadAt, 1)
				atomic.AddUint64(&d.metricReadAtBytes, uint64(cmdLength))
				atomic.AddUint64(&d.metricReadAtTime, uint64(time.Since(ctime)))
			} else {
				select {
				case d.fatal <- err:
				default:
				}
			}
			d.pendingResponses.Done()
		}()
	} else {
		ctime := time.Now()
		err := performRead(cmdHandle, cmdFrom, cmdLength)
		if err == nil {
			atomic.AddUint64(&d.metricReadAt, 1)
			atomic.AddUint64(&d.metricReadAtBytes, uint64(cmdLength))
			atomic.AddUint64(&d.metricReadAtTime, uint64(time.Since(ctime)))
		}
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
	if d.logger != nil {
		d.logger.Trace().
			Str("device", d.dev).
			Uint64("cmdHandle", cmdHandle).
			Uint64("cmdFrom", cmdFrom).
			Uint32("cmdLength", cmdLength).
			Msg("nbd cmdWrite")
	}

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

	d.shuttingDownLock.Lock()
	if !d.shuttingDown {
		d.pendingResponses.Add(1)
	} else {
		d.shuttingDownLock.Unlock()
		return ErrShuttingDown
	}
	d.shuttingDownLock.Unlock()

	if d.asyncWrites {
		go func() {
			ctime := time.Now()
			err := performWrite(cmdHandle, cmdFrom, cmdLength, cmdData)
			if err == nil {
				atomic.AddUint64(&d.metricWriteAt, 1)
				atomic.AddUint64(&d.metricWriteAtBytes, uint64(cmdLength))
				atomic.AddUint64(&d.metricWriteAtTime, uint64(time.Since(ctime)))
			} else {
				select {
				case d.fatal <- err:
				default:
				}
			}
			d.pendingResponses.Done()
		}()
	} else {
		ctime := time.Now()
		err := performWrite(cmdHandle, cmdFrom, cmdLength, cmdData)
		if err == nil {
			atomic.AddUint64(&d.metricWriteAt, 1)
			atomic.AddUint64(&d.metricWriteAtBytes, uint64(cmdLength))
			atomic.AddUint64(&d.metricWriteAtTime, uint64(time.Since(ctime)))
		}
		d.pendingResponses.Done()
		return err
	}
	return nil
}

/**
 * cmdTrim
 *
 */
func (d *Dispatch) cmdTrim(handle uint64, _ uint64, _ uint32) error {
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
