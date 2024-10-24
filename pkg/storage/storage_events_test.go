package storage_test

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/volatilitymonitor"
	"github.com/loopholelabs/silo/pkg/storage/waitingcache"
	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
)

type SomeStorage struct {
	storage.StorageProviderWithEvents
}

func NewSomeStorage() *SomeStorage {
	return &SomeStorage{}
}

func (ss *SomeStorage) ReadAt([]byte, int64) (int, error) {
	return 0, nil
}

func (ss *SomeStorage) WriteAt([]byte, int64) (int, error) {
	return 0, nil
}

func (ss *SomeStorage) Size() uint64 {
	return 0
}

func (ss *SomeStorage) Flush() error {
	return nil
}

func (ss *SomeStorage) Close() error {
	return nil
}

func (ss *SomeStorage) CancelWrites(int64, int64) {
}

func (ss *SomeStorage) UUID() []uuid.UUID {
	return nil
}

// Only exists in SomeStorage
func (ss *SomeStorage) SomeName() string {
	return "SomeStorage"
}

type SomeStorageNoEvents struct {
}

func NewSomeStorageNoEvents() *SomeStorageNoEvents {
	return &SomeStorageNoEvents{}
}

func (ss *SomeStorageNoEvents) ReadAt([]byte, int64) (int, error) {
	return 0, nil
}

func (ss *SomeStorageNoEvents) WriteAt([]byte, int64) (int, error) {
	return 0, nil
}

func (ss *SomeStorageNoEvents) Size() uint64 {
	return 0
}

func (ss *SomeStorageNoEvents) Flush() error {
	return nil
}

func (ss *SomeStorageNoEvents) Close() error {
	return nil
}

func (ss *SomeStorageNoEvents) CancelWrites(int64, int64) {
}

func (ss *SomeStorageNoEvents) UUID() []uuid.UUID {
	return nil
}

func TestStorageEvents(t *testing.T) {
	ss := NewSomeStorage()

	var wg sync.WaitGroup
	wg.Add(1)
	ok := storage.AddSiloEventNotification(ss, "testing", func(event storage.EventType, data storage.EventData) storage.EventReturnData {
		// Do something here
		assert.Equal(t, storage.EventType("testing"), event)
		assert.Equal(t, "HELLO WORLD", data.(string))
		wg.Done()

		return "SOMETHING"
	})
	assert.True(t, ok)

	data := storage.SendSiloEvent(ss, "testing", storage.EventData("HELLO WORLD"))
	assert.Equal(t, 1, len(data))
	assert.Equal(t, "SOMETHING", data[0].(string))

	wg.Wait()

	// Try doing it on something that doesn't support events

	ssnl := NewSomeStorageNoEvents()

	ok = storage.AddSiloEventNotification(ssnl, "testing", func(from storage.EventType, to storage.EventData) storage.EventReturnData {
		assert.Fail(t, "shouldn't happen")
		return nil
	})
	assert.False(t, ok)
	data = storage.SendSiloEvent(ssnl, "testing", nil)
	assert.Nil(t, data)

}

/**
 * Make sure events work as we expect when storage providers are chained for migration
 *
 */

type module_data struct {
	prov            storage.StorageProvider
	events_received uint64
}

type test_data struct {
	name             string
	insert_handler   map[int]bool
	expected_returns map[int]int
	expected_counts  map[int]uint64
}

func TestStorageEventsForModules(tt *testing.T) {
	test_cases := []test_data{
		{
			name: "all handlers",
			insert_handler: map[int]bool{0: true, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true,
				9: true, 10: true, 11: true, 12: true, 13: true, 14: true, 15: true, 16: true, 17: true, 18: true},
			expected_returns: map[int]int{
				0: 1, 1: 2, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, 11: 11, 12: 12, 13: 13, 14: 14, 15: 15, 16: 16, 17: 17, 18: 17},
			expected_counts: map[int]uint64{
				0: 19, 1: 17, 2: 1, 3: 16, 4: 15, 5: 14, 6: 13, 7: 12, 8: 11, 9: 10, 10: 9, 11: 8, 12: 7, 13: 6, 14: 5, 15: 4, 16: 3, 17: 1, 18: 1},
		},
		{
			name: "one handler",
			insert_handler: map[int]bool{0: true, 1: false, 2: false, 3: false, 4: false, 5: false, 6: false, 7: false, 8: false,
				9: false, 10: false, 11: false, 12: false, 13: false, 14: false, 15: false, 16: false, 17: false, 18: false},
			expected_returns: map[int]int{
				0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1, 9: 1, 10: 1, 11: 1, 12: 1, 13: 1, 14: 1, 15: 1, 16: 1, 17: 1, 18: 1},
			expected_counts: map[int]uint64{
				0: 19, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0, 16: 0, 17: 0, 18: 0},
		},
	}

	for _, td := range test_cases {
		tt.Run(td.name, func(t *testing.T) {

			size := 1024 * 1024
			blockSize := 4096

			all_modules := make([]*module_data, 0)

			// Add a module into our list
			add_module := func(s storage.StorageProvider) {
				i := len(all_modules)

				mod_data := &module_data{
					prov:            s,
					events_received: 0,
				}
				all_modules = append(all_modules, mod_data)

				if td.insert_handler[i] {
					// Register an event notification on the module.
					ok := storage.AddSiloEventNotification(s, "some_event", func(event storage.EventType, data storage.EventData) storage.EventReturnData {
						assert.Equal(t, event, storage.EventType("some_event"))
						assert.Equal(t, data, storage.EventData("some_data"))
						atomic.AddUint64(&mod_data.events_received, 1)
						return fmt.Sprintf("RETURN DATA %d", len(all_modules))
					})
					assert.True(t, ok)
				}
			}

			// Start with some memory storage, and register a handler on it
			sourceStorageMem := sources.NewMemoryStorage(size)
			add_module(sourceStorageMem)

			// dirty tracker
			sourceDirtyLocal, sourceDirtyRemote := dirtytracker.NewDirtyTracker(sourceStorageMem, blockSize)
			add_module(sourceDirtyLocal)
			add_module(sourceDirtyRemote)

			mod1 := modules.NewArtificialLatency(sourceDirtyLocal, 0, 0, 0, 0)
			add_module(mod1)
			mod2, err := modules.NewBinLog(mod1, "binlog_file")
			assert.NoError(t, err)
			t.Cleanup(func() {
				os.Remove("binlog_file")
			})
			add_module(mod2)
			mod3 := modules.NewBlockSplitter(mod2, blockSize)
			add_module(mod3)
			mod4 := modules.NewCopyOnWrite(mod3, sources.NewMemoryStorage(size), blockSize)
			add_module(mod4)
			mod5 := modules.NewDummyTracker(mod4, blockSize)
			add_module(mod5)
			mod6 := modules.NewFilterRedundantWrites(mod5, nil, 0)
			add_module(mod6)
			mod7 := modules.NewHooks(mod6)
			add_module(mod7)
			mod8 := modules.NewLockable(mod7)
			add_module(mod8)
			mod9 := modules.NewLogger(mod8, "prefix")
			add_module(mod9)
			mod10 := modules.NewMetrics(mod9)
			add_module(mod10)
			mod11, err := modules.NewRaid([]storage.StorageProvider{mod10})
			assert.NoError(t, err)
			add_module(mod11)
			mod12 := modules.NewReadOnlyGate(mod11)
			add_module(mod12)
			mod13, err := modules.NewShardedStorage(size, size, func(index int, size int) (storage.StorageProvider, error) {
				return mod12, nil
			})
			assert.NoError(t, err)
			add_module(mod13)

			mod14 := volatilitymonitor.NewVolatilityMonitor(mod13, blockSize, time.Second)
			add_module(mod14)
			mod15, mod16 := waitingcache.NewWaitingCache(mod14, blockSize)
			add_module(mod15)
			add_module(mod16)

			// Now send events to various parts of the chain, and make sure the handlers receive the events.
			for i, mod := range all_modules {
				r := storage.SendSiloEvent(mod.prov, storage.EventType("some_event"), storage.EventData("some_data"))
				assert.NotNil(t, r)
				assert.Equal(t, td.expected_returns[i], len(r))
			}

			// Check the modules got the right number of events on them...
			for i, mod := range all_modules {
				assert.Equal(t, td.expected_counts[i], mod.events_received)
			}
		})
	}
}
