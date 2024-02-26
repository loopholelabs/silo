package expose

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
)

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

// IOctl call info
type IoctlCall struct {
	Cmd   uintptr
	Value uintptr
}

type ExposedStorageNBD struct {
	dev             string
	num_connections int
	timeout         uint64
	size            uint64
	block_size      uint64
	flags           uint64
	socketPairs     [][2]int
	device_file     uintptr
	dispatch        *Dispatch
	prov            storage.StorageProvider
}

func NewExposedStorageNBD(prov storage.StorageProvider, dev string, num_connections int, timeout uint64, size uint64, block_size uint64, flags uint64) *ExposedStorageNBD {
	return &ExposedStorageNBD{
		prov:            prov,
		dev:             dev,
		num_connections: num_connections,
		timeout:         timeout,
		size:            size,
		block_size:      block_size,
		flags:           flags,
		dispatch:        NewDispatch(),
	}
}

func (n *ExposedStorageNBD) setSizes(fp uintptr, size uint64, block_size uint64, flags uint64) error {
	//read_only := ((flags & NBD_FLAG_READ_ONLY) == 1)

	// TODO: Should we throw an error if size isn't a multiple, or should we do something else...

	// Round up
	size_blocks := (size + block_size - 1) / block_size
	_, _, en := syscall.Syscall(syscall.SYS_IOCTL, fp, NBD_SET_SIZE_BLOCKS, uintptr(size_blocks))
	if en != 0 {
		return fmt.Errorf("error setting blocks %d", size_blocks)
	}
	_, _, en = syscall.Syscall(syscall.SYS_IOCTL, fp, NBD_SET_BLKSIZE, uintptr(block_size))
	if en != 0 {
		return fmt.Errorf("error setting blocksize %d", block_size)
	}

	syscall.Syscall(syscall.SYS_IOCTL, fp, NBD_CLEAR_SOCK, 0)

	syscall.Syscall(syscall.SYS_IOCTL, fp, NBD_SET_FLAGS, uintptr(flags))

	// 	if (ioctl(nbd, BLKROSET, (unsigned long) &read_only) < 0)
	return nil
}

func (n *ExposedStorageNBD) setTimeout(fp uintptr, timeout uint64) error {
	_, _, en := syscall.Syscall(syscall.SYS_IOCTL, fp, NBD_SET_TIMEOUT, uintptr(timeout))
	if en != 0 {
		return fmt.Errorf("error setting timeout %d", timeout)
	}
	return nil
}

func (n *ExposedStorageNBD) finishSock(fp uintptr, sock int) error {
	_, _, en := syscall.Syscall(syscall.SYS_IOCTL, fp, NBD_SET_SOCK, uintptr(sock))
	if en == syscall.EBUSY {
		return fmt.Errorf("kernel doesn't support multiple connections")
	} else if en != 0 {
		return fmt.Errorf("error setting socket")
	}
	return nil
}

/**
 * Check if the nbd connection is up and running...
 *
 */
func (n *ExposedStorageNBD) checkConn(dev string) error {
	_, err := os.ReadFile(fmt.Sprintf("/sys/block/%s/pid", dev))
	/*
		if err == nil {
			//fmt.Printf("Connection %s\n", data)
		}
	*/
	return err
}

func (n *ExposedStorageNBD) Handle() error {
	device_file := fmt.Sprintf("/dev/%s", n.dev)

	fp, err := os.OpenFile(device_file, os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	n.device_file = fp.Fd()

	// Create the socket pairs, and setup NBD options.
	for i := 0; i < n.num_connections; i++ {
		sockPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
		if err != nil {
			return err
		}
		n.socketPairs = append(n.socketPairs, sockPair)

		// Start reading commands on the socket and dispatching them to our provider
		go func(fd int) {
			n.dispatch.Handle(fd, n.prov)
		}(sockPair[1])

		if i == 0 {
			n.setSizes(fp.Fd(), n.size, n.block_size, n.flags)
			n.setTimeout(fp.Fd(), n.timeout)
		}
		n.finishSock(fp.Fd(), sockPair[0])
	}

	go func() {
		for {
			err := n.checkConn(n.dev)
			if err == nil {
				break
			}
			// Sleep a bit
			// nanosleep(&req, NULL)
			time.Sleep(100000000 * time.Nanosecond)
		}
		_, err := os.OpenFile(device_file, os.O_RDWR, 0600)
		if err != nil {
			fmt.Printf("Could not open device for updating partition table\n")
		}
	}()

	// Now do it...
	_, _, en := syscall.Syscall(syscall.SYS_IOCTL, fp.Fd(), NBD_DO_IT, 0)

	// Clear the socket
	syscall.Syscall(syscall.SYS_IOCTL, fp.Fd(), NBD_CLEAR_SOCK, 0)

	if en != 0 {
		return fmt.Errorf("error after DO_IT %d", en)
	}

	// Close the device...
	fp.Close()
	return nil
}

// Wait until it's ready...
func (n *ExposedStorageNBD) WaitReady() error {
	for {
		err := n.checkConn(n.dev)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}
	return nil
}

func (n *ExposedStorageNBD) Shutdown() error {

	fmt.Printf("Closing sockets...\n")
	// Close all the socket pairs...
	for _, v := range n.socketPairs {
		err := syscall.Close(v[1])
		if err != nil {
			return err
		}
	}

	fmt.Printf("NBD_DISCONNECT\n")
	_, _, en := syscall.Syscall(syscall.SYS_IOCTL, uintptr(n.device_file), NBD_DISCONNECT, 0)
	if en != 0 {
		return fmt.Errorf("error disconnecting %d", en)
	}
	fmt.Printf("NBD_CLEAR_SOCK\n")
	_, _, en = syscall.Syscall(syscall.SYS_IOCTL, uintptr(n.device_file), NBD_CLEAR_SOCK, 0)
	if en != 0 {
		return fmt.Errorf("error clearing sock %d", en)
	}

	// Wait for the pid to go away
	fmt.Printf("Wait pid\n")
	for {
		err := n.checkConn(n.dev)
		if err != nil {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}

	return nil
}

type Dispatch struct {
	ASYNC_READS      bool
	ASYNC_WRITES     bool
	fp               *os.File
	responseHeader   []byte
	writeLock        sync.Mutex
	prov             storage.StorageProvider
	fatal            chan error
	pendingResponses sync.WaitGroup
}

func NewDispatch() *Dispatch {
	d := &Dispatch{
		ASYNC_WRITES:   true,
		ASYNC_READS:    true,
		responseHeader: make([]byte, 16),
		fatal:          make(chan error, 8),
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

	//	fmt.Printf("WriteResponse %x -> %d\n", respHandle, len(chunk))

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
	return nil
}

/**
 * This dispatches incoming NBD requests sequentially to the provider.
 *
 */
func (d *Dispatch) Handle(fd int, prov storage.StorageProvider) error {
	d.prov = prov
	d.fp = os.NewFile(uintptr(fd), "unix")

	// Speed read and dispatch...

	BUFFER_SIZE := 4 * 1024 * 1024
	buffer := make([]byte, BUFFER_SIZE)
	wp := 0

	request := Request{}

	for {
		//		fmt.Printf("Calling read...\n")
		n, err := d.fp.Read(buffer[wp:])
		if err != nil {
			fmt.Printf("Error %v\n", err)
			return err
		}
		wp += n

		//		fmt.Printf("Read %d\n", n)

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

				if request.Magic != NBD_REQUEST_MAGIC {
					return fmt.Errorf("received invalid MAGIC")
				}

				if request.Type == NBD_CMD_DISCONNECT {
					fmt.Printf(" -> CMD_DISCONNECT\n")
					return nil // All done
				} else if request.Type == NBD_CMD_FLUSH {
					return fmt.Errorf("not supported: Flush")
				} else if request.Type == NBD_CMD_READ {
					//					fmt.Printf("READ %x %d\n", request.Handle, request.Length)
					rp += 28
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
					data := make([]byte, request.Length)
					copy(data, buffer[rp:rp+int(request.Length)])
					rp += int(request.Length)
					//fmt.Printf("WRITE %x %d\n", request.Handle, request.Length)
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
