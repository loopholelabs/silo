package swarming

import (
	"context"

	"github.com/loopholelabs/silo/pkg/storage"
)

// A local provider for a block of data. It's stored in a storage.Provider at a certain Offset with a certain Length
type ProviderHBL struct {
	Offset   int64
	Size     int64
	Provider storage.Provider
}

func (p *ProviderHBL) GetBytes(ctx context.Context) ([]byte, error) {
	errs := make(chan error, 1)
	buffer := make([]byte, p.Size)

	// Perform the read in a goroutine
	go func() {
		n, err := p.Provider.ReadAt(buffer, p.Offset)
		if err == nil {
			buffer = buffer[:n]
		}
		errs <- err
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e := <-errs:
		return buffer, e
	}
}
