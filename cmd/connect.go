package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/spf13/cobra"
)

var (
	cmdConnect = &cobra.Command{
		Use:   "connect",
		Short: "Start up connect",
		Long:  ``,
		Run:   runConnect,
	}
)

var connect_addr string
var connect_expose_dev bool

var exposed []storage.ExposedStorage

func init() {
	rootCmd.AddCommand(cmdConnect)
	cmdConnect.Flags().StringVarP(&connect_addr, "addr", "a", "localhost:5170", "Address to serve from")
	cmdConnect.Flags().BoolVarP(&connect_expose_dev, "expose", "e", false, "Expose as an nbd devices")
}

func runConnect(ccmd *cobra.Command, args []string) {
	fmt.Printf("Starting silo connect from source %s\n", connect_addr)

	exposed = make([]storage.ExposedStorage, 0)

	// Connect to the source
	con, err := net.Dial("tcp", connect_addr)
	if err != nil {
		panic("Error connecting")
	}

	// Wrap the connection in a protocol, and handle incoming devices
	pro := protocol.NewProtocolRW(context.TODO(), con, con, handleIncomingDevice)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c

		for _, e := range exposed {
			fmt.Printf("\nShutting down cleanly...\n")
			shutdown(e)
		}
		os.Exit(1)
	}()

	// Let the protocol do its thing.
	err = pro.Handle()
	if err != nil {
		fmt.Printf("Silo protocol error %v\n", err)
	}
}

// Handle a new incoming device
func handleIncomingDevice(pro protocol.Protocol, dev uint32) {
	var destStorage storage.StorageProvider
	var destWaitingLocal *modules.WaitingCacheLocal
	var destWaitingRemote *modules.WaitingCacheRemote
	var dest *modules.FromProtocol

	destWaitingRemoteFactory := func(di *protocol.DevInfo) storage.StorageProvider {
		fmt.Printf("= %d = Received DevInfo name=%s size=%d blocksize=%d\n", dev, di.Name, di.Size, di.BlockSize)

		cr := func(s int) storage.StorageProvider {
			return sources.NewMemoryStorage(s)
		}
		// Setup some sharded memory storage (for concurrent write speed)
		destStorage = modules.NewShardedStorage(int(di.Size), int(di.Size/1024), cr)

		destWaitingLocal, destWaitingRemote = modules.NewWaitingCache(destStorage, int(di.BlockSize))

		// Connect the waitingCache to the FromProtocol
		destWaitingLocal.NeedAt = func(offset int64, length int32) {
			dest.NeedAt(offset, length)
		}

		destWaitingLocal.DontNeedAt = func(offset int64, length int32) {
			dest.DontNeedAt(offset, length)
		}

		// Expose it if we should...
		if connect_expose_dev {
			p, err := setup(destWaitingLocal, false)
			if err != nil {
				fmt.Printf("Error during setup (expose nbd) %v\n", err)
			}
			exposed = append(exposed, p)
		}

		fmt.Printf("Returning destWaitingRemote...\n")
		return destWaitingRemote
	}

	// TODO: Need to allow for DevInfo on different IDs better here...
	dest = modules.NewFromProtocol(dev, destWaitingRemoteFactory, pro)

	go dest.HandleSend(context.TODO())
	go dest.HandleReadAt()
	go dest.HandleWriteAt()
	go dest.HandleDevInfo()
	go dest.HandleEvent(func(e protocol.EventType) {
		fmt.Printf("= %d = Event %s\n", dev, protocol.EventsByType[e])
	})

	go dest.HandleDirtyList(func(dirty []uint) {
		fmt.Printf("= %d = LIST OF DIRTY BLOCKS %v\n", dev, dirty)
		destWaitingLocal.DirtyBlocks(dirty)
	})
}
