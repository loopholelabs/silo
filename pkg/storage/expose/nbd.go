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
	"github.com/google/uuid"
	"github.com/loopholelabs/logging/types"
	"github.com/loopholelabs/silo/pkg/storage"
)

const NBDDefaultBlockSize = 4096

const NBDAlignSectorSize = 512

var DefaultConfig = &Config{
	NumConnections: 8,
	Timeout:        0,
	BlockSize:      NBDDefaultBlockSize,
	AsyncReads:     true,
	AsyncWrites:    true,
}

type Config struct {
	Logger         types.RootLogger
	NumConnections int
	Timeout        time.Duration
	BlockSize      uint64
	AsyncReads     bool
	AsyncWrites    bool
}

func (c *Config) WithLogger(l types.RootLogger) *Config {
	return &Config{Logger: l,
		NumConnections: c.NumConnections,
		Timeout:        c.Timeout,
		BlockSize:      c.BlockSize,
		AsyncReads:     c.AsyncReads,
		AsyncWrites:    c.AsyncWrites}
}

func (c *Config) WithNumConnections(cons int) *Config {
	return &Config{Logger: c.Logger,
		NumConnections: cons,
		Timeout:        c.Timeout,
		BlockSize:      c.BlockSize,
		AsyncReads:     c.AsyncReads,
		AsyncWrites:    c.AsyncWrites}
}

func (c *Config) WithTimeout(t time.Duration) *Config {
	return &Config{Logger: c.Logger,
		NumConnections: c.NumConnections,
		Timeout:        t,
		BlockSize:      c.BlockSize,
		AsyncReads:     c.AsyncReads,
		AsyncWrites:    c.AsyncWrites}
}

func (c *Config) WithBlockSize(bs uint64) *Config {
	return &Config{Logger: c.Logger,
		NumConnections: c.NumConnections,
		Timeout:        c.Timeout,
		BlockSize:      bs,
		AsyncReads:     c.AsyncReads,
		AsyncWrites:    c.AsyncWrites}
}

func (c *Config) WithAsyncReads(t bool) *Config {
	return &Config{Logger: c.Logger,
		NumConnections: c.NumConnections,
		Timeout:        c.Timeout,
		BlockSize:      c.BlockSize,
		AsyncReads:     t,
		AsyncWrites:    c.AsyncWrites}
}

func (c *Config) WithAsyncWrites(t bool) *Config {
	return &Config{Logger: c.Logger,
		NumConnections: c.NumConnections,
		Timeout:        c.Timeout,
		BlockSize:      c.BlockSize,
		AsyncReads:     c.AsyncReads,
		AsyncWrites:    t}
}

/**
 * Exposes a storage provider as an nbd device using netlink
 *
 */
type ExposedStorageNBDNL struct {
	config   *Config
	uuid     uuid.UUID
	ctx      context.Context
	cancelfn context.CancelFunc
	size     uint64

	socks       []io.Closer
	prov        storage.Provider
	provLock    sync.RWMutex
	deviceIndex int
	dispatchers []*Dispatch
}

func NewExposedStorageNBDNL(prov storage.Provider, conf *Config) *ExposedStorageNBDNL {
	size := prov.Size()

	// The size must be a multiple of sector size
	size = NBDAlignSectorSize * ((size + NBDAlignSectorSize - 1) / NBDAlignSectorSize)

	ctx, cancelfn := context.WithCancel(context.TODO())

	return &ExposedStorageNBDNL{
		config:   conf,
		uuid:     uuid.New(),
		ctx:      ctx,
		cancelfn: cancelfn,
		prov:     prov,
		size:     size,
		socks:    make([]io.Closer, 0),
	}
}

func (n *ExposedStorageNBDNL) SetProvider(prov storage.Provider) {
	n.provLock.Lock()
	n.prov = prov
	n.provLock.Unlock()
}

// Impl StorageProvider here so we can route calls to provider
func (n *ExposedStorageNBDNL) ReadAt(buffer []byte, offset int64) (int, error) {
	n.provLock.RLock()
	defer n.provLock.RUnlock()
	return n.prov.ReadAt(buffer, offset)
}

func (n *ExposedStorageNBDNL) WriteAt(buffer []byte, offset int64) (int, error) {
	n.provLock.RLock()
	defer n.provLock.RUnlock()
	return n.prov.WriteAt(buffer, offset)
}

func (n *ExposedStorageNBDNL) Flush() error {
	n.provLock.RLock()
	defer n.provLock.RUnlock()
	return n.prov.Flush()
}

func (n *ExposedStorageNBDNL) Size() uint64 {
	n.provLock.RLock()
	defer n.provLock.RUnlock()
	return n.prov.Size()
}

func (n *ExposedStorageNBDNL) Close() error {
	n.provLock.RLock()
	defer n.provLock.RUnlock()
	return n.prov.Close()
}

func (n *ExposedStorageNBDNL) CancelWrites(offset int64, length int64) {
	n.provLock.RLock()
	defer n.provLock.RUnlock()
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
		for i := 0; i < n.config.NumConnections; i++ {
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

			d := NewDispatch(n.ctx, n.uuid.String(), n.config.Logger, serverc, n)
			d.asyncReads = n.config.AsyncReads
			d.asyncWrites = n.config.AsyncWrites
			// Start reading commands on the socket and dispatching them to our provider
			go func() {
				err := d.Handle()
				if n.config.Logger != nil {
					n.config.Logger.Trace().
						Str("uuid", n.uuid.String()).
						Err(err).
						Msg("nbd dispatch completed")
				}
			}()
			n.socks = append(n.socks, serverc)
			socks = append(socks, client)
			n.dispatchers = append(n.dispatchers, d)
		}
		var opts []nbdnl.ConnectOption
		opts = append(opts, nbdnl.WithBlockSize(n.config.BlockSize))
		opts = append(opts, nbdnl.WithTimeout(n.config.Timeout))
		opts = append(opts, nbdnl.WithDeadconnTimeout(n.config.Timeout))

		serverFlags := nbdnl.FlagHasFlags | nbdnl.FlagCanMulticonn

		idx, err := nbdnl.Connect(nbdnl.IndexAny, socks, n.size, 0, serverFlags, opts...)
		if err == nil {
			n.deviceIndex = int(idx)
			break
		}

		if n.config.Logger != nil {
			n.config.Logger.Trace().
				Str("uuid", n.uuid.String()).
				Err(err).
				Msg("error from nbdnl.Connect")
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

	if n.config.Logger != nil {
		n.config.Logger.Trace().
			Str("uuid", n.uuid.String()).
			Int("deviceIndex", n.deviceIndex).
			Msg("Waiting for nbd device to init")
	}

	// Wait until it's connected...
	for {
		s, err := nbdnl.Status(uint32(n.deviceIndex))
		if err == nil && s.Connected {
			break
		}
		time.Sleep(100 * time.Nanosecond)
	}

	if n.config.Logger != nil {
		n.config.Logger.Trace().
			Str("uuid", n.uuid.String()).
			Int("deviceIndex", n.deviceIndex).
			Msg("nbd device connected")
	}

	return nil
}

func (n *ExposedStorageNBDNL) Shutdown() error {
	if n.config.Logger != nil {
		n.config.Logger.Trace().
			Str("uuid", n.uuid.String()).
			Int("deviceIndex", n.deviceIndex).
			Msg("nbd device shutdown initiated")
	}

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

	if n.config.Logger != nil {
		n.config.Logger.Trace().
			Str("uuid", n.uuid.String()).
			Int("deviceIndex", n.deviceIndex).
			Msg("nbd device disconnected")
	}

	return nil
}
