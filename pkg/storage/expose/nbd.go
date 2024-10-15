package expose

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Merovius/nbd/nbdnl"
	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/rs/zerolog/log"
)

const NBD_DEFAULT_BLOCK_SIZE = 4096

/**
 * Exposes a storage provider as an nbd device using netlink
 *
 */
type ExposedStorageNBDNL struct {
	ctx             context.Context
	cancelfn        context.CancelFunc
	num_connections int
	timeout         time.Duration
	size            uint64
	block_size      uint64

	socks        []io.Closer
	prov_lock    sync.RWMutex
	prov         storage.StorageProvider
	device_index int
	async        bool
	dispatchers  []*Dispatch
}

func NewExposedStorageNBDNL(prov storage.StorageProvider, numConnections int, timeout time.Duration, size uint64, blockSize uint64, async bool) *ExposedStorageNBDNL {

	// The size must be a multiple of sector size
	alignTo := uint64(512)
	size = alignTo * ((size + alignTo - 1) / alignTo)

	ctx, cancelfn := context.WithCancel(context.TODO())

	return &ExposedStorageNBDNL{
		ctx:             ctx,
		cancelfn:        cancelfn,
		prov:            prov,
		num_connections: numConnections,
		timeout:         timeout,
		size:            size,
		block_size:      blockSize,
		socks:           make([]io.Closer, 0),
		async:           async,
	}
}

func (n *ExposedStorageNBDNL) SetProvider(prov storage.StorageProvider) {
	n.prov_lock.Lock()
	defer n.prov_lock.Unlock()
	n.prov = prov
	log.Trace().Msg("NBD device.SetProvider called.")
}

// Impl StorageProvider here so we can route calls to provider
func (i *ExposedStorageNBDNL) ReadAt(buffer []byte, offset int64) (int, error) {
	i.prov_lock.RLock()
	defer i.prov_lock.RUnlock()
	return i.prov.ReadAt(buffer, offset)
}

func (i *ExposedStorageNBDNL) WriteAt(buffer []byte, offset int64) (int, error) {
	i.prov_lock.RLock()
	defer i.prov_lock.RUnlock()
	return i.prov.WriteAt(buffer, offset)
}

func (i *ExposedStorageNBDNL) Flush() error {
	i.prov_lock.RLock()
	defer i.prov_lock.RUnlock()
	return i.prov.Flush()
}

func (i *ExposedStorageNBDNL) Size() uint64 {
	i.prov_lock.RLock()
	defer i.prov_lock.RUnlock()
	return i.prov.Size()
}

func (i *ExposedStorageNBDNL) Close() error {
	i.prov_lock.RLock()
	defer i.prov_lock.RUnlock()
	return i.prov.Close()
}

func (i *ExposedStorageNBDNL) CancelWrites(offset int64, length int64) {
	i.prov_lock.RLock()
	defer i.prov_lock.RUnlock()
	i.prov.CancelWrites(offset, length)
}

func (n *ExposedStorageNBDNL) Device() string {
	return fmt.Sprintf("nbd%d", n.device_index)
}

func (n *ExposedStorageNBDNL) Init() error {
	log.Trace().Msg("NBD Device Init()")

	for {

		socks := make([]*os.File, 0)

		n.dispatchers = make([]*Dispatch, 0)

		// Create the socket pairs
		for i := 0; i < n.num_connections; i++ {
			sockPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
			if err != nil {
				log.Error().Err(err).Msg("NBD Could not create socket pair")
				return err
			}

			client := os.NewFile(uintptr(sockPair[0]), "client")
			server := os.NewFile(uintptr(sockPair[1]), "server")
			serverc, err := net.FileConn(server)
			if err != nil {
				log.Error().Err(err).Msg("NBD Could not create FileConn")
				return err
			}
			server.Close()

			d := NewDispatch(n.ctx, serverc, n)
			d.ASYNC_READS = n.async
			d.ASYNC_WRITES = n.async

			// Start reading commands on the socket and dispatching them to our provider
			go func() {
				err = d.Handle()
				if err != nil && err != context.Canceled && err != io.EOF {
					log.Error().Err(err).Msg("NBD Dispatch returned")
				}
			}()
			n.socks = append(n.socks, serverc)
			socks = append(socks, client)
			n.dispatchers = append(n.dispatchers, d)
		}
		var opts []nbdnl.ConnectOption
		opts = append(opts, nbdnl.WithBlockSize(uint64(n.block_size)))
		opts = append(opts, nbdnl.WithTimeout(n.timeout))
		opts = append(opts, nbdnl.WithDeadconnTimeout(n.timeout))

		serverFlags := nbdnl.FlagHasFlags | nbdnl.FlagCanMulticonn

		idx, err := nbdnl.Connect(nbdnl.IndexAny, socks, n.size, 0, serverFlags, opts...)
		if err == nil {
			log.Info().Uint32("index", idx).Msg("NBD Device created")
			n.device_index = int(idx)
			break
		}

		// FIXME: This is just for info. Usually it's an odd BADF error, which a retry seems to fix.
		log.Trace().Err(err).Msg("NBD Error from nbdnl.Connect")

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
		s, err := nbdnl.Status(uint32(n.device_index))
		if err == nil && s.Connected {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}

	log.Trace().Int("index", n.device_index).Msg("NBD connected")
	return nil
}

func (n *ExposedStorageNBDNL) Shutdown() error {
	log.Trace().Int("index", n.device_index).Msg("NBD shutdown")

	// First cancel the context, which will stop waiting on pending readAt/writeAt...
	n.cancelfn()

	// Now wait for any pending responses to be sent
	for _, d := range n.dispatchers {
		d.Wait()
	}

	log.Trace().Int("index", n.device_index).Msg("NBD disconnect")
	// Now ask to disconnect
	err := nbdnl.Disconnect(uint32(n.device_index))
	if err != nil {
		log.Error().Err(err).Int("index", n.device_index).Msg("NBD disconnect error")
		return err
	}

	// Close all the socket pairs...
	for _, v := range n.socks {
		err = v.Close()
		if err != nil {
			log.Error().Err(err).Int("index", n.device_index).Msg("NBD close socket")
			return err
		}
	}

	log.Trace().Int("index", n.device_index).Msg("Waiting for disconnection")

	// Wait until it's completely disconnected...
	for {
		s, err := nbdnl.Status(uint32(n.device_index))
		if err == nil && !s.Connected {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}

	log.Trace().Int("index", n.device_index).Msg("NBD shutdown finished")
	return nil
}
