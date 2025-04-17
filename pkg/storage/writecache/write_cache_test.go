package writecache

import (
	"fmt"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/stretchr/testify/assert"
)

func TestWriteCache(t *testing.T) {
	prov := sources.NewMemoryStorage(1024 * 1024 * 1024) // 1GB memory
	met := modules.NewMetrics(prov)
	bl, err := modules.NewBinLogReplay("memory_binlog", met)
	assert.NoError(t, err)
	err1, err2 := bl.ExecuteAll()
	assert.NoError(t, err1)
	assert.NoError(t, err2)

	// Now try the same thing but with a WriteCache

	met.ShowStats("memory")

	MB := int64(1024 * 1024)

	for _, cacheSize := range []int64{1 * MB, 4 * MB, 64 * MB, 256 * MB, 1024 * MB} {

		provWC := sources.NewMemoryStorage(1024 * 1024 * 1024) // 1GB memory
		metWC := modules.NewMetrics(provWC)

		wc := NewWriteCache(1024*1024, metWC, cacheSize)

		blWC, err := modules.NewBinLogReplay("memory_binlog", wc)
		assert.NoError(t, err)
		err1, err2 = blWC.ExecuteAll()
		assert.NoError(t, err1)
		assert.NoError(t, err2)

		// Flush the data...
		err = wc.Flush()
		assert.NoError(t, err2)

		// Make sure the data is the same
		eq, err := storage.Equals(prov, provWC, 1024*1024)
		assert.NoError(t, err2)
		assert.True(t, eq)

		// Look at the stats...
		metWC.ShowStats(fmt.Sprintf("memoryWC_%d", cacheSize))
	}
}
