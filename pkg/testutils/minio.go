package testutils

import (
	"fmt"
	"net/http"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func SetupMinio(cleanup func(func())) string {

	pool, err := dockertest.NewPool("")
	if err != nil {
		panic(err)
	}

	options := &dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		/*
			PortBindings: map[docker.Port][]docker.PortBinding{
				"9000/tcp": []docker.PortBinding{{HostPort: "9000"}},
			},
		*/
		Env: []string{"MINIO_ACCESS_KEY=silosilo", "MINIO_SECRET_KEY=silosilo"},
	}

	resource, err := pool.RunWithOptions(options, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})

	if err != nil {
		panic(err)
	}

	cleanup(func() {
		fmt.Printf("Cleaning up minio docker...\n")
		err := pool.Purge(resource)
		if err != nil {
			panic(err)
		}
	})

	err = resource.Expire(180)
	if err != nil {
		panic(err)
	}
	PORT_9000 := resource.GetPort("9000/tcp")

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
