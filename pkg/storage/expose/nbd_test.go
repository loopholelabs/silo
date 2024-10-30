package expose

import (
	"crypto/rand"
	"fmt"
	"os"
	"os/user"
	"sync"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestNBDNLDevice(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	var n *ExposedStorageNBDNL
	defer func() {
		err := n.Shutdown()
		assert.NoError(t, err)
	}()

	size := 4096 * 1024 * 1024
	prov := sources.NewMemoryStorage(size)

	n = NewExposedStorageNBDNL(prov, 8, 0, uint64(size), 4096, true)

	err = n.Init()
	assert.NoError(t, err)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", n.deviceIndex), os.O_RDWR, 0666)
			assert.NoError(t, err)

			// Try doing a read...
			off := 12
			buffer := make([]byte, 4096)
			num, err := devfile.ReadAt(buffer, int64(off))
			assert.NoError(t, err)
			assert.Equal(t, len(buffer), num)
			devfile.Close()
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestNBDNLDeviceBlocksizes(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	blockSizes := []int{512, 1 * 1024, 2 * 1024, 4 * 1024}

	size := 4 * 1024 * 1024
	prov := sources.NewMemoryStorage(size)

	for _, bs := range blockSizes {
		t.Run(fmt.Sprintf("blockSize-%d", bs), func(tt *testing.T) {
			var n *ExposedStorageNBDNL
			defer func() {
				err := n.Shutdown()
				assert.NoError(t, err)
			}()

			n = NewExposedStorageNBDNL(prov, 8, 0, uint64(size), uint64(bs), true)

			err = n.Init()
			assert.NoError(tt, err)
		})
	}
}

func TestNBDNLDeviceSmall(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	var ndev *ExposedStorageNBDNL
	defer func() {
		err := ndev.Shutdown()
		assert.NoError(t, err)
	}()

	size := 900
	prov := sources.NewMemoryStorage(size)

	b := make([]byte, 900)
	_, err = rand.Read(b)
	assert.NoError(t, err)
	n, err := prov.WriteAt(b, 0)
	assert.NoError(t, err)
	assert.Equal(t, 900, n)

	ndev = NewExposedStorageNBDNL(prov, 1, 0, uint64(size), 4096, true)

	err = ndev.Init()
	assert.NoError(t, err)

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", ndev.deviceIndex), os.O_RDWR, 0666)
	assert.NoError(t, err)

	// Try doing a read...
	buffer := make([]byte, 900)
	num, err := devfile.ReadAt(buffer, 0)
	assert.NoError(t, err)
	assert.Equal(t, 900, num)
	devfile.Close()

	// Make sure the data is equal
	assert.Equal(t, b, buffer)
}

func TestNBDNLDeviceUnalignedPartialRead(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	var ndev *ExposedStorageNBDNL
	defer func() {
		err := ndev.Shutdown()
		assert.NoError(t, err)
	}()

	size := 8 * 1024
	prov := sources.NewMemoryStorage(size)

	b := make([]byte, size)
	_, err = rand.Read(b)
	assert.NoError(t, err)
	n, err := prov.WriteAt(b, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	ndev = NewExposedStorageNBDNL(prov, 1, 0, uint64(size), 4096, true)

	err = ndev.Init()
	assert.NoError(t, err)

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", ndev.deviceIndex), os.O_RDWR, 0666)
	assert.NoError(t, err)

	// Try doing a read...
	buffer := make([]byte, 88)
	num, err := devfile.ReadAt(buffer, 7)
	assert.NoError(t, err)
	assert.Equal(t, len(buffer), num)
	devfile.Close()

	// Make sure the data is equal
	assert.Equal(t, b[7:7+88], buffer)
}

func TestNBDNLDeviceUnalignedPartialWrite(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	var ndev *ExposedStorageNBDNL
	defer func() {
		err := ndev.Shutdown()
		assert.NoError(t, err)
	}()

	size := 900
	prov := sources.NewMemoryStorage(size)
	ndev = NewExposedStorageNBDNL(prov, 1, 0, uint64(size), 4096, true)

	err = ndev.Init()
	assert.NoError(t, err)

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", ndev.deviceIndex), os.O_RDWR, 0666)
	assert.NoError(t, err)

	// Try doing a read...
	buffer := make([]byte, 800)
	_, err = rand.Read(buffer)
	assert.NoError(t, err)
	num, err := devfile.WriteAt(buffer, 10)
	assert.NoError(t, err)
	assert.Equal(t, 800, num)
	err = devfile.Sync()
	assert.NoError(t, err)
	err = devfile.Close()
	assert.NoError(t, err)

	// Make sure the write got through to our provider

	b := make([]byte, 900)
	num, err = prov.ReadAt(b, 0)
	assert.NoError(t, err)
	assert.Equal(t, 900, num)

	// Make sure the data is equal
	assert.Equal(t, b[10:810], buffer)
}

func TestNBDNLDeviceShutdownRead(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	var n *ExposedStorageNBDNL

	size := 4096 * 1024 * 1024
	prov := sources.NewMemoryStorage(size)

	hooks := modules.NewHooks(prov)

	var readWG sync.WaitGroup
	readWG.Add(1)

	hooks.PreRead = func(_ []byte, offset int64) (bool, int, error) {
		if offset == 0x9000 {
			readWG.Done()
			time.Sleep(10 * time.Second)
		}
		return false, 0, nil
	}

	n = NewExposedStorageNBDNL(hooks, 8, 0, uint64(size), 4096, true)
	err = n.Init()
	assert.NoError(t, err)

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", n.deviceIndex), os.O_RDWR, 0666)
	assert.NoError(t, err)

	t.Cleanup(func() {
		devfile.Close()
	})

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		// Try doing a read...
		off := 0x9000
		buffer := make([]byte, 4096)
		ctime := time.Now()
		_, err := devfile.ReadAt(buffer, int64(off))
		assert.Error(t, err) // input/output error
		assert.WithinDuration(t, ctime, time.Now(), time.Second)
		wg.Done()
	}()

	readWG.Wait()
	// This will be called during the ReadAt, but may take a while to completely wait for nbd subsystem to finish
	go func() {
		err = n.Shutdown()
		assert.NoError(t, err)
	}()

	// Now wait for the read to finish
	wg.Wait()

}

func TestNBDNLDeviceShutdownWrite(t *testing.T) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Username != "root" {
		fmt.Printf("Cannot run test unless we are root.\n")
		return
	}

	var n *ExposedStorageNBDNL

	size := 4096 * 1024 * 1024
	prov := sources.NewMemoryStorage(size)

	hooks := modules.NewHooks(prov)

	var readWG sync.WaitGroup
	readWG.Add(1)

	hooks.PreWrite = func(_ []byte, offset int64) (bool, int, error) {
		if offset == 0x9000 {
			readWG.Done()
			time.Sleep(10 * time.Second)
		}
		return false, 0, nil
	}

	n = NewExposedStorageNBDNL(hooks, 8, 0, uint64(size), 4096, true)
	err = n.Init()
	assert.NoError(t, err)

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", n.deviceIndex), os.O_RDWR, 0666)
	assert.NoError(t, err)

	t.Cleanup(func() {
		devfile.Close()
	})

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		// Try doing a write...
		off := 0x9000
		buffer := make([]byte, 4096)
		ctime := time.Now()
		_, err := devfile.WriteAt(buffer, int64(off))
		assert.NoError(t, err)
		err = devfile.Sync()
		assert.Error(t, err) // input/output error
		assert.WithinDuration(t, ctime, time.Now(), time.Second)
		wg.Done()
	}()

	readWG.Wait()
	// This will be called during the WriteAt, but may take a while to completely wait for nbd subsystem to finish
	go func() {
		err = n.Shutdown()
		assert.NoError(t, err)
	}()

	// Now wait for the write to finish
	wg.Wait()

}
