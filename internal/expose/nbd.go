package expose

import (
	"fmt"
	"os"
	"syscall"

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

const NBD_CMD_READ = 0
const NBD_CMD_WRITE = 1
const NBD_CMD_DISCONNECT = 2
const NBD_CMD_FLUSH = 3
const NBD_CMD_TRIM = 4

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

type NBDExposedStorage struct {
	device       *os.File
	fp           uintptr
	socketPair   [2]int
	readyChannel chan error
	dispatcher   NBDDispatcher
}

// Want to swap in and out different versions here so we can benchmark
type NBDDispatcher interface {
	Handle(fd int, prov storage.StorageProvider) error
	Name() string
	Wait()
}

/**
 * Handle storage requests using the provider
 *
 */
func (s *NBDExposedStorage) Handle(prov storage.StorageProvider) error {
	// Handle incoming requests...
	go func() {
		err := s.dispatcher.Handle(s.socketPair[0], prov)
		if err != nil {
			fmt.Printf("RequestHandler quit unexpectedly %v\n", err)
			//			fmt.Printf("Sleeping for a min...\n")
			//			time.Sleep(time.Minute)
			// Shutdown properly...
			s.Shutdown()
		}
		/*
			fmt.Printf("Close socketPair[0] %d\n", s.socketPair[0])
			err = syscall.Close(s.socketPair[0])
			if err != nil {
				fmt.Printf("RequestHandler close error %v\n", err)
			}
		*/
	}()

	// Issue ioctl calls to set it up
	calls := []IoctlCall{
		//		{NBD_SET_BLKSIZE, 4096},
		{NBD_SET_TIMEOUT, 0},
		{NBD_PRINT_DEBUG, 1},
		{NBD_SET_SIZE, uintptr(prov.Size())},
		{NBD_CLEAR_QUE, 0},
		{NBD_CLEAR_SOCK, 0},
		{NBD_SET_SOCK, uintptr(s.socketPair[1])},
		{NBD_SET_FLAGS, NBD_FLAG_SEND_TRIM},
		{NBD_DO_IT, 0},
	}

	for _, c := range calls {
		if c.Cmd == NBD_DO_IT {
			s.readyChannel <- nil
		}
		//		fmt.Printf("IOCTL %d %d\n", c.Cmd, c.Value)
		_, _, en := syscall.Syscall(syscall.SYS_IOCTL, s.fp, c.Cmd, c.Value)
		if en != 0 {
			err := fmt.Errorf("syscall error %s", syscall.Errno(en))
			if c.Cmd != NBD_DO_IT {
				s.readyChannel <- err
			}
			return err
		}
	}
	return nil
}

/**
 * Wait until things are running
 * This can only be called ONCE
 */
func (s *NBDExposedStorage) WaitReady() error {
	//	fmt.Printf("WaitReady\n")
	// Wait for a ready signal
	return <-s.readyChannel
}

/**
 * Shutdown the nbd device
 *
 */
func (s *NBDExposedStorage) Shutdown() error {
	s.dispatcher.Wait()

	calls := []IoctlCall{
		{NBD_CLEAR_QUE, 0},
		{NBD_DISCONNECT, 0},
		{NBD_CLEAR_SOCK, 0},
	}

	for _, c := range calls {
		fmt.Printf("IOCTL %d %d\n", c.Cmd, c.Value)
		_, _, en := syscall.Syscall(syscall.SYS_IOCTL, s.fp, c.Cmd, c.Value)
		if en != 0 {
			return fmt.Errorf("syscall error %s", syscall.Errno(en))
		}
	}
	fmt.Printf("CLOSING DEVICE...\n")
	/*
		fmt.Printf("Close socketPair[1] %d\n", s.socketPair[1])
		err := syscall.Close(s.socketPair[1])
		if err != nil {
			return err
		}
	*/
	return s.device.Close()
}

/**
 * Create a new NBD device
 *
 */
func NewNBD(d NBDDispatcher, dev string) (storage.ExposedStorage, error) {
	// Create a pair of sockets to communicate with the NBD device over
	//	fmt.Printf("Create socketpair\n")
	sockPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}
	//	fmt.Printf("Socketpair %d %d\n", sockPair[0], sockPair[1])

	// Open the nbd device
	//	fmt.Printf("Open device %s\n", dev)
	fp, err := os.OpenFile(dev, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	es := &NBDExposedStorage{
		socketPair:   sockPair,
		device:       fp,
		fp:           fp.Fd(),
		readyChannel: make(chan error),
		dispatcher:   d,
	}

	return es, nil
}
