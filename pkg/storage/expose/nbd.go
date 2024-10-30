package expose

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/Merovius/nbd/nbdnl"
	"github.com/loopholelabs/silo/pkg/storage"
)

const NBD_DEFAULT_BLOCK_SIZE = 4096

const NBD_ALIGN_SECTOR_SIZE = 512

/**
 * Exposes a storage provider as an nbd device using netlink
 *
 */
type ExposedStorageNBDNL struct {
	ctx            context.Context
	cancelfn       context.CancelFunc
	numConnections int
	timeout        time.Duration
	size           uint64
	blockSize      uint64

	socks       []io.Closer
	prov        storage.StorageProvider
	deviceIndex int
	async       bool
	dispatchers []*Dispatch
}

func NewExposedStorageNBDNL(prov storage.StorageProvider, numConnections int, timeout time.Duration, size uint64, blockSize uint64, async bool) *ExposedStorageNBDNL {

	// The size must be a multiple of sector size
	size = NBD_ALIGN_SECTOR_SIZE * ((size + NBD_ALIGN_SECTOR_SIZE - 1) / NBD_ALIGN_SECTOR_SIZE)

	ctx, cancelfn := context.WithCancel(context.TODO())

	return &ExposedStorageNBDNL{
		ctx:            ctx,
		cancelfn:       cancelfn,
		prov:           prov,
		numConnections: numConnections,
		timeout:        timeout,
		size:           size,
		blockSize:      blockSize,
		socks:          make([]io.Closer, 0),
		async:          async,
	}
}

func (n *ExposedStorageNBDNL) SetProvider(prov storage.StorageProvider) {
	n.prov = prov
}

// Impl StorageProvider here so we can route calls to provider
func (n *ExposedStorageNBDNL) ReadAt(buffer []byte, offset int64) (int, error) {
	return n.prov.ReadAt(buffer, offset)
}

func (n *ExposedStorageNBDNL) WriteAt(buffer []byte, offset int64) (int, error) {
	return n.prov.WriteAt(buffer, offset)
}

func (n *ExposedStorageNBDNL) Flush() error {
	return n.prov.Flush()
}

func (n *ExposedStorageNBDNL) Size() uint64 {
	return n.prov.Size()
}

func (n *ExposedStorageNBDNL) Close() error {
	return n.prov.Close()
}

func (n *ExposedStorageNBDNL) CancelWrites(offset int64, length int64) {
	n.prov.CancelWrites(offset, length)
}

func (n *ExposedStorageNBDNL) Device() string {
	return fmt.Sprintf("nbd%d", n.deviceIndex)
}

func (n *ExposedStorageNBDNL) Init() error {

	for {

		socks := make([]*os.File, 0)

		n.dispatchers = make([]*Dispatch, 0)

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

			d := NewDispatch(n.ctx, serverc, n)
			d.asyncReads = n.async
			d.asyncWrites = n.async
			// Start reading commands on the socket and dispatching them to our provider
			go func() {
				_ = d.Handle()
			}()
			n.socks = append(n.socks, serverc)
			socks = append(socks, client)
			n.dispatchers = append(n.dispatchers, d)
		}
		var opts []nbdnl.ConnectOption
		opts = append(opts, nbdnl.WithBlockSize(n.blockSize))
		opts = append(opts, nbdnl.WithTimeout(n.timeout))
		opts = append(opts, nbdnl.WithDeadconnTimeout(n.timeout))

		serverFlags := nbdnl.FlagHasFlags | nbdnl.FlagCanMulticonn

		idx, err := nbdnl.Connect(nbdnl.IndexAny, socks, n.size, 0, serverFlags, opts...)
		if err == nil {
			n.deviceIndex = int(idx)
			break
		}

		// Sometimes (rare), there seems to be a BADF error here. Lets just retry for now...
		// Close things down and try again...
		for _, s := range socks {
			s.Close()
		}

		if strings.Contains(err.Error(), "invalid argument") {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}

	// Wait until it's connected...
	for {
		s, err := nbdnl.Status(uint32(n.deviceIndex))
		if err == nil && s.Connected {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}

	return nil
}

func (n *ExposedStorageNBDNL) Shutdown() error {
	// First cancel the context, which will stop waiting on pending readAt/writeAt...
	n.cancelfn()

	// Now wait for any pending responses to be sent
	for _, d := range n.dispatchers {
		d.Wait()
	}

	// Now ask to disconnect
	err := nbdnl.Disconnect(uint32(n.deviceIndex))
	if err != nil {
		return err
	}

	// Close all the socket pairs...
	for _, v := range n.socks {
		err = v.Close()
		if err != nil {
			return err
		}
	}

	// Wait until it's completely disconnected...
	for {
		s, err := nbdnl.Status(uint32(n.deviceIndex))
		if err == nil && !s.Connected {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}

	return nil
}
