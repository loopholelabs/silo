package sources

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) string {

	pool, err := dockertest.NewPool("")
	if err != nil {
		panic(err)
	}

	options := &dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "--console-address", ":9001", "/data"},
		PortBindings: map[dc.Port][]dc.PortBinding{
			"9000/tcp": []dc.PortBinding{{HostPort: "9000"}},
			"9001/tcp": []dc.PortBinding{{HostPort: "9001"}},
		},
		Env: []string{"MINIO_ACCESS_KEY=silosilo", "MINIO_SECRET_KEY=silosilo", "MINIO_DEFAULT_BUCKETS=silosilo"},
	}

	resource, err := pool.RunWithOptions(options)

	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		err := pool.Purge(resource)
		if err != nil {
			panic(err)
		}
	})

	//	resource.Expire(180)
	PORT_9000 := resource.GetPort("9000/tcp")
	PORT_9001 := resource.GetPort("9001/tcp")

	fmt.Printf("Minio running on ports 9000=%s 9001=%s\n", PORT_9000, PORT_9001)

	err = pool.Retry(func() error {
		url := fmt.Sprintf("http://localhost:%s/minio/health/live", PORT_9000)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	})

	if err != nil {
		panic(err)
	}
	return PORT_9000
}

func TestS3Storage(t *testing.T) {
	PORT_9000 := setup(t)

	//PORT_9000 := "9000"

	fmt.Printf("minio setup, lets run stuff against it...9000=%s\n", PORT_9000)
	// Now do some things

	size := 1 * 1024 * 1024
	blockSize := size / 128
	s3store, err := NewS3StorageCreate(fmt.Sprintf("localhost:%s", PORT_9000), "silosilo", "silosilo", "silosilo", "file", uint64(size), blockSize)
	assert.NoError(t, err)

	buffer := make([]byte, 128*1024)
	rand.Read(buffer)
	_, err = s3store.WriteAt(buffer, 80)
	assert.NoError(t, err)

	// Now read it back...
	buffer2 := make([]byte, size)
	n, err := s3store.ReadAt(buffer2, 0)
	assert.Equal(t, size, n)
	assert.NoError(t, err)

	//	assert.Equal(t, buffer, buffer2[80:80+len(buffer)])

	err = s3store.Close()
	assert.NoError(t, err)
}
