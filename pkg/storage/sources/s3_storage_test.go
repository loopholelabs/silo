package sources

import (
	"fmt"
	"net"
	"testing"

	"github.com/ory/dockertest"
)

func setup(t *testing.T) (string, string) {

	pool, err := dockertest.NewPool("")
	if err != nil {
		panic(err)
	}
	resource, err := pool.Run("docker.io/bitnami/minio", "2022", []string{"MINIO_ROOT_USER=silosilo", "MINIO_ROOT_PASSWORD=silosilo", "MINIO_DEFAULT_BUCKETS=silosilo"})
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		err := pool.Purge(resource)
		if err != nil {
			panic(err)
		}
	})

	resource.Expire(180)
	var PORT_9000 string
	var PORT_9001 string

	err = pool.Retry(func() error {
		PORT_9000 = resource.GetPort("9000/tcp")
		PORT_9001 = resource.GetPort("9001/tcp")
		// ping to ensure that the server is up and running
		_, err := net.Dial("tcp", net.JoinHostPort("localhost", PORT_9000))
		return err
	})

	return PORT_9000, PORT_9001
}

func TestS3Storage(t *testing.T) {
	PORT_9000, PORT_9001 := setup(t)

	fmt.Printf("minio setup, lets run stuff against it...9000=%s 9001=%s\n", PORT_9000, PORT_9001)
	// Now do some things
}
