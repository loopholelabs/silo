package expose

import (
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
	num_connections int
	timeout         uint64
	size            uint64
	block_size      uint64

	socks       []io.Closer
	device_file uintptr
	prov        storage.StorageProvider
	DevIndex    int
	async       bool
}

func NewExposedStorageNBDNL(prov storage.StorageProvider, num_connections int, timeout uint64, size uint64, block_size uint64, async bool) *ExposedStorageNBDNL {
	return &ExposedStorageNBDNL{
		prov:            prov,
		num_connections: num_connections,
		timeout:         timeout,
		size:            size,
		block_size:      block_size,
		socks:           make([]io.Closer, 0),
		async:           async,
	}
}

func (n *ExposedStorageNBDNL) Handle() error {

	socks := make([]*os.File, 0)

	// Create the socket pairs
	for i := 0; i < n.num_connections; i++ {
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
	opts = append(opts, nbdnl.WithBlockSize(uint64(n.block_size)))
	opts = append(opts, nbdnl.WithTimeout(100*time.Millisecond))
	opts = append(opts, nbdnl.WithDeadconnTimeout(100*time.Millisecond))

	serverFlags := nbdnl.ServerFlags(0) //
	serverFlags = nbdnl.FlagHasFlags | nbdnl.FlagCanMulticonn

	idx, err := nbdnl.Connect(nbdnl.IndexAny, socks, n.size, 0, serverFlags, opts...)
	if err != nil {
		return err
	}

	n.DevIndex = int(idx)
	return nil
}

// Wait until it's connected...
func (n *ExposedStorageNBDNL) WaitReady() error {
	for {
		s, err := nbdnl.Status(uint32(n.DevIndex))
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
	err := nbdnl.Disconnect(uint32(n.DevIndex))
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
