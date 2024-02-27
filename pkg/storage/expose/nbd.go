package expose

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
	c, err := os.ReadFile(fmt.Sprintf("/sys/block/%s/pid", dev))
	if err != nil {
		return err
	}
	_, err = strconv.Atoi(strings.Trim(string(c), "\r\n\t "))
	if err != nil {
		fmt.Printf("PARSE ERROR %s\n", c)
		return err
	}
	/*
		process_id := os.Getpid()
		if pid != process_id {
			return fmt.Errorf("Process ID not correct %d %d", pid, process_id)
		}
	*/
	//fmt.Printf("checkConn %s\n", c)
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

		rwc := os.NewFile(uintptr(sockPair[1]), "unix")
		d := NewDispatch(rwc, n.prov)

		// Start reading commands on the socket and dispatching them to our provider
		go d.Handle()

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

	/*
		fmt.Printf("Closing sockets...\n")
		// Close all the socket pairs...
		for _, v := range n.socketPairs {
			err := syscall.Close(v[1])
			if err != nil {
				return err
			}
		}
	*/
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
