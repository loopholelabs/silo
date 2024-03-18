package expose

import (
	"crypto/rand"
	"fmt"
	"os"
	"os/user"
	"sync"
	"testing"

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
			devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", n.devIndex), os.O_RDWR, 0666)
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
	rand.Read(b)
	n, err := prov.WriteAt(b, 0)
	assert.NoError(t, err)
	assert.Equal(t, 900, n)

	ndev = NewExposedStorageNBDNL(prov, 1, 0, uint64(size), 4096, true)

	err = ndev.Init()
	assert.NoError(t, err)

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", ndev.devIndex), os.O_RDWR, 0666)
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
	rand.Read(b)
	n, err := prov.WriteAt(b, 0)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	ndev = NewExposedStorageNBDNL(prov, 1, 0, uint64(size), 4096, true)

	err = ndev.Init()
	assert.NoError(t, err)

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", ndev.devIndex), os.O_RDWR, 0666)
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

	devfile, err := os.OpenFile(fmt.Sprintf("/dev/nbd%d", ndev.devIndex), os.O_RDWR, 0666)
	assert.NoError(t, err)

	// Try doing a read...
	buffer := make([]byte, 800)
	rand.Read(buffer)
	num, err := devfile.WriteAt(buffer, 10)
	assert.NoError(t, err)
	assert.Equal(t, 800, num)
	devfile.Sync()
	devfile.Close()

	// Make sure the write got through to our provider

	b := make([]byte, 900)
	num, err = prov.ReadAt(b, 0)
	assert.NoError(t, err)
	assert.Equal(t, 900, num)

	// Make sure the data is equal
	assert.Equal(t, b[10:810], buffer)
}
