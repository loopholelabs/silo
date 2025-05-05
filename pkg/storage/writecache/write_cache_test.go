package writecache

import (
	"fmt"
	"testing"
	"time"

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

	fmt.Printf("No WriteCache        \t %s\n", showStats(met.GetMetrics()))

	MB := int64(1024 * 1024)

	type cacheConfig struct {
		name              string
		minData           int64
		maxData           int64
		period            time.Duration
		minimizeReadBytes bool
	}

	for _, conf := range []cacheConfig{
		{name: "1mb_basic", minData: 0, maxData: 1 * MB, period: 10 * time.Second, minimizeReadBytes: false},
		{name: "5-10mb", minData: 5 * MB, maxData: 10 * MB, period: 10 * time.Second, minimizeReadBytes: false},
		{name: "50-100mb", minData: 50 * MB, maxData: 100 * MB, period: 10 * time.Second, minimizeReadBytes: false},
		{name: "250-500mb", minData: 250 * MB, maxData: 500 * MB, period: 100 * time.Millisecond, minimizeReadBytes: false},
		{name: "250-500mb MINREADS", minData: 250 * MB, maxData: 500 * MB, period: 100 * time.Millisecond, minimizeReadBytes: true},
	} {
		provWC := sources.NewMemoryStorage(1024 * 1024 * 1024) // 1GB memory
		metWC := modules.NewMetrics(provWC)

		wc := NewWriteCache(1024*1024, metWC, &Config{
			MinData:           conf.minData,
			MaxData:           conf.maxData,
			FlushPeriod:       conf.period,
			MinimizeReadBytes: conf.minimizeReadBytes,
		})

		blWC, err := modules.NewBinLogReplay("memory_binlog", wc)
		assert.NoError(t, err)
		err1, err2 = blWC.ExecuteAll()
		assert.NoError(t, err1)
		assert.NoError(t, err2)

		// Flush the data...
		err = wc.Flush()
		assert.NoError(t, err)

		// Make sure the data is the same
		eq, err := storage.Equals(prov, provWC, 1024*1024)
		assert.NoError(t, err)
		assert.True(t, eq)

		// Look at the stats...
		fmt.Printf("WriteCache_%s\t %s\n", conf.name, showStats(metWC.GetMetrics()))
	}
}

func showStats(met *modules.MetricsSnapshot) string {
	return fmt.Sprintf("IOPS %d (%d r %d w)\tData %d (%d r %d w)",
		met.ReadOps+met.WriteOps,
		met.ReadOps,
		met.WriteOps,
		met.ReadBytes+met.WriteBytes,
		met.ReadBytes,
		met.WriteBytes)
}
