package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/expose"
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

// Address to connect to
var connect_addr string

// Should we expose each device as an nbd device?
var connect_expose_dev bool

// Should we also mount the devices
var connect_mount_dev bool

// List of ExposedStorage so they can be cleaned up on exit.
var dst_exposed []storage.ExposedStorage

func init() {
	rootCmd.AddCommand(cmdConnect)
	cmdConnect.Flags().StringVarP(&connect_addr, "addr", "a", "localhost:5170", "Address to serve from")
	cmdConnect.Flags().BoolVarP(&connect_expose_dev, "expose", "e", false, "Expose as an nbd devices")
	cmdConnect.Flags().BoolVarP(&connect_mount_dev, "mount", "m", false, "Mount the nbd devices")
}

/**
 * Connect to a silo source and stream whatever devices are available.
 *
 */
func runConnect(ccmd *cobra.Command, args []string) {
	fmt.Printf("Starting silo connect from source %s\n", connect_addr)

	dst_exposed = make([]storage.ExposedStorage, 0)

	// Handle shutdown gracefully to disconnect any exposed devices correctly.
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c

		for _, e := range dst_exposed {
			dst_device_shutdown(e)
		}
		os.Exit(1)
	}()

	// Connect to the source
	fmt.Printf("Dialing source at %s...\n", connect_addr)
	con, err := net.Dial("tcp", connect_addr)
	if err != nil {
		panic("Error connecting to source")
	}

	// Wrap the connection in a protocol, and handle incoming devices
	pro := protocol.NewProtocolRW(context.TODO(), con, con, handleIncomingDevice)

	// Let the protocol do its thing.
	err = pro.Handle()
	if err != nil && err != io.EOF {
		fmt.Printf("Silo protocol error %v\n", err)
	}

	fmt.Printf("Migrations completed.\n")

	// We should pause here, to allow the user to do things with the devices
	time.Sleep(10 * time.Minute)

	// Shutdown any storage exposed as devices
	for _, e := range dst_exposed {
		dst_device_shutdown(e)
	}
}

// Handle a new incoming device. This is called when a packet is received for a device we haven't heard about before.
func handleIncomingDevice(pro protocol.Protocol, dev uint32) {
	var destStorage storage.StorageProvider
	var destWaitingLocal *modules.WaitingCacheLocal
	var destWaitingRemote *modules.WaitingCacheRemote
	var dest *modules.FromProtocol

	// This is a storage factory which will be called when we recive DevInfo.
	storageFactory := func(di *protocol.DevInfo) storage.StorageProvider {
		fmt.Printf("= %d = Received DevInfo name=%s size=%d blocksize=%d\n", dev, di.Name, di.Size, di.BlockSize)

		// You can change this to use sources.NewFileStorage etc etc
		cr := func(s int) storage.StorageProvider {
			return sources.NewMemoryStorage(s)
		}
		// Setup some sharded memory storage (for concurrent write speed)
		destStorage = modules.NewShardedStorage(int(di.Size), int(di.Size/1024), cr)

		// Use a WaitingCache which will wait for migration blocks, send priorities etc
		// A WaitingCache has two ends - local and remote.
		destWaitingLocal, destWaitingRemote = modules.NewWaitingCache(destStorage, int(di.BlockSize))

		// Connect the waitingCache to the FromProtocol.
		// Note that since these are hints, errors don't matter too much.
		destWaitingLocal.NeedAt = func(offset int64, length int32) {
			dest.NeedAt(offset, length)
		}

		destWaitingLocal.DontNeedAt = func(offset int64, length int32) {
			dest.DontNeedAt(offset, length)
		}

		// Expose this storage as a device if requested
		if connect_expose_dev {
			p, err := dst_device_setup(destWaitingLocal)
			if err != nil {
				fmt.Printf("= %d = Error during setup (expose nbd) %v\n", dev, err)
			}
			dst_exposed = append(dst_exposed, p)
		}
		return destWaitingRemote
	}

	dest = modules.NewFromProtocol(dev, storageFactory, pro)

	// Handle sending
	go dest.HandleSend(context.TODO())
	go dest.HandleReadAt()
	go dest.HandleWriteAt()
	go dest.HandleDevInfo()

	// Handle events from the source
	go dest.HandleEvent(func(e protocol.EventType) {
		fmt.Printf("= %d = Event %s\n", dev, protocol.EventsByType[e])
		// Check we have all data...
		if e == protocol.EventCompleted {
			available, total := destWaitingLocal.Availability()
			fmt.Printf("= %d = Availability (%d/%d)\n", dev, available, total)
		}
	})

	// Handle dirty list by invalidating local waiting cache
	go dest.HandleDirtyList(func(dirty []uint) {
		fmt.Printf("= %d = LIST OF DIRTY BLOCKS %v\n", dev, dirty)
		destWaitingLocal.DirtyBlocks(dirty)
	})
}

// Called to setup an exposed storage device
func dst_device_setup(prov storage.StorageProvider) (storage.ExposedStorage, error) {
	p := expose.NewExposedStorageNBDNL(prov, 1, 0, prov.Size(), 4096, true)

	err := p.Init()
	if err != nil {
		fmt.Printf("p.Init returned %v\n", err)
		return nil, err
	}

	device := p.Device()
	fmt.Printf("* Device ready on /dev/%s\n", device)

	// We could also mount the device, but we should do so inside a goroutine, so that it doesn't block things...
	if connect_mount_dev {
		err = os.Mkdir(fmt.Sprintf("/mnt/mount%s", device), 0600)
		if err != nil {
			return nil, fmt.Errorf("Error mkdir %v", err)
		}

		go func() {
			fmt.Printf("Mounting device...")
			cmd := exec.Command("mount", "-r", fmt.Sprintf("/dev/%s", device), fmt.Sprintf("/mnt/mount%s", device))
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Could not mount device %v\n", err)
				return
			}
			fmt.Printf("* Device is mounted at /mnt/mount%s\n", device)
		}()
	}

	return p, nil
}

// Called to shutdown an exposed storage device
func dst_device_shutdown(p storage.ExposedStorage) error {
	device := p.Device()

	fmt.Printf("Shutdown %s\n", device)
	if connect_mount_dev {
		cmd := exec.Command("umount", fmt.Sprintf("/dev/%s", device))
		err := cmd.Run()
		if err != nil {
			return err
		}
		err = os.Remove(fmt.Sprintf("/mnt/mount%s", device))
		if err != nil {
			return err
		}
	}

	err := p.Shutdown()
	if err != nil {
		return err
	}
	return nil
}
