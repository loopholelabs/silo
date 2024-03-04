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

func NewExposedStorageNBDNL(prov storage.StorageProvider, num_connections int, timeout uint64, size uint64, block_size uint64, async bool) *ExposedStorageNBDNL {
	return &ExposedStorageNBDNL{
		prov:           prov,
		numConnections: num_connections,
		timeout:        timeout,
		size:           size,
		blockSize:      block_size,
		socks:          make([]io.Closer, 0),
		async:          async,
	}
}

func (n *ExposedStorageNBDNL) Device() string {
	return fmt.Sprintf("nbd%d", n.devIndex)
}

func (n *ExposedStorageNBDNL) Handle() error {

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

	serverFlags := nbdnl.ServerFlags(0) //
	serverFlags = nbdnl.FlagHasFlags | nbdnl.FlagCanMulticonn

	idx, err := nbdnl.Connect(nbdnl.IndexAny, socks, n.size, 0, serverFlags, opts...)
	if err != nil {
		return err
	}

	n.devIndex = int(idx)
	return nil
}

// Wait until it's connected... (Handle must have been called already)
func (n *ExposedStorageNBDNL) WaitReady() error {
	for {
		s, err := nbdnl.Status(uint32(n.devIndex))
		if err == nil && s.Connected {
			//			fmt.Printf("NBD %d connected\n", n.DevIndex)
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

	return nil
}
