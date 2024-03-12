package expose

import (
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/Merovius/nbd/nbdnl"
	"github.com/loopholelabs/silo/pkg/storage"
)

/**
 * Exposes a storage provider as an nbd device using netlink
 *
 */
type ExposedStorageNBDNL struct {
	numConnections int
	timeout        uint64
	size           uint64
	blockSize      uint64

	socks      []io.Closer
	deviceFile uintptr
	prov       storage.StorageProvider
	devIndex   int
	async      bool
}

func NewExposedStorageNBDNL(prov storage.StorageProvider, numConnections int, timeout uint64, size uint64, blockSize uint64, async bool) *ExposedStorageNBDNL {

	// The size must be a multiple of sector size
	alignTo := uint64(512)
	size = alignTo * ((size + alignTo - 1) / alignTo)

	return &ExposedStorageNBDNL{
		prov:           prov,
		numConnections: numConnections,
		timeout:        timeout,
		size:           size,
		blockSize:      blockSize,
		socks:          make([]io.Closer, 0),
		async:          async,
	}
}

func (n *ExposedStorageNBDNL) Device() string {
	return fmt.Sprintf("nbd%d", n.devIndex)
}

func (n *ExposedStorageNBDNL) Init() error {

	for {

		socks := make([]*os.File, 0)

		// Create the socket pairs
		for i := 0; i < n.numConnections; i++ {
			sockPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
			if err != nil {
				return err
			}

			client := os.NewFile(uintptr(sockPair[0]), "client")
			server := os.NewFile(uintptr(sockPair[1]), "server")
			serverc, err := net.FileConn(server)
			if err != nil {
				return err
			}
			server.Close()

			//		fmt.Printf("[%d] Socket pair %d -> %d %v %v %v\n", i, sockPair[0], sockPair[1], client, server, serverc)

			d := NewDispatch(serverc, n.prov)
			d.ASYNC_READS = n.async
			d.ASYNC_WRITES = n.async
			// Start reading commands on the socket and dispatching them to our provider
			go d.Handle()
			n.socks = append(n.socks, serverc)
			socks = append(socks, client)
		}
		var opts []nbdnl.ConnectOption
		opts = append(opts, nbdnl.WithBlockSize(uint64(n.blockSize)))
		opts = append(opts, nbdnl.WithTimeout(100*time.Millisecond))
		opts = append(opts, nbdnl.WithDeadconnTimeout(100*time.Millisecond))

		serverFlags := nbdnl.FlagHasFlags | nbdnl.FlagCanMulticonn

		idx, err := nbdnl.Connect(nbdnl.IndexAny, socks, n.size, 0, serverFlags, opts...)
		if err == nil {
			n.devIndex = int(idx)
			break
		}

		//		fmt.Printf("\n\nError from nbdnl.Connect %v\n\n", err)

		// Sometimes (rare), there seems to be a BADF error here. Lets just retry for now...
		// Close things down and try again...
		for _, s := range socks {
			s.Close()
		}

		//		fmt.Printf("\n\nRetrying...\n\n")
		time.Sleep(50 * time.Millisecond)
	}

	// Wait until it's connected...
	for {
		s, err := nbdnl.Status(uint32(n.devIndex))
		if err == nil && s.Connected {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}

	return nil
}

func (n *ExposedStorageNBDNL) Shutdown() error {

	// Ask to disconnect
	err := nbdnl.Disconnect(uint32(n.devIndex))
	if err != nil {
		return err
	}

	//	fmt.Printf("Closing sockets...\n")
	// Close all the socket pairs...
	for _, v := range n.socks {
		err = v.Close()
		if err != nil {
			return err
		}
	}

	// Wait until it's disconnected...
	for {
		s, err := nbdnl.Status(uint32(n.devIndex))
		if err == nil && !s.Connected {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}

	return nil
}
