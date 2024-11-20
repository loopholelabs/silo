package modules_test

import (
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestTimeout(t *testing.T) {
	mem := sources.NewMemoryStorage(1024 * 1024)
	timeout := modules.NewTimeout(mem, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond)

	buffer := make([]byte, 1024)
	_, err := timeout.ReadAt(buffer, 0)
	assert.NoError(t, err)

	_, err = timeout.WriteAt(buffer, 0)
	assert.NoError(t, err)

	err = timeout.Flush()
	assert.NoError(t, err)

	err = timeout.Close()
	assert.NoError(t, err)

	// Now with artificial latency, we expect these calls to fail with ErrTimeout

	latency := modules.NewArtificialLatency(mem, 5*time.Second, 0, 5*time.Second, 0, 5*time.Second, 5*time.Second)
	timeoutLatency := modules.NewTimeout(latency, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond)

	_, err = timeoutLatency.ReadAt(buffer, 0)
	assert.ErrorIs(t, modules.ErrTimeout, err)

	_, err = timeoutLatency.WriteAt(buffer, 0)
	assert.ErrorIs(t, modules.ErrTimeout, err)

	err = timeoutLatency.Flush()
	assert.ErrorIs(t, modules.ErrTimeout, err)

	err = timeoutLatency.Close()
	assert.ErrorIs(t, modules.ErrTimeout, err)

}
