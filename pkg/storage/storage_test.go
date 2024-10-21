package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
)

type SomeStorage struct {
	StorageProviderWithEvents
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

type SomeStorageNoLife struct {
}

func NewSomeStorageNoLife() *SomeStorageNoLife {
	return &SomeStorageNoLife{}
}

func (ss *SomeStorageNoLife) ReadAt([]byte, int64) (int, error) {
	return 0, nil
}

func (ss *SomeStorageNoLife) WriteAt([]byte, int64) (int, error) {
	return 0, nil
}

func (ss *SomeStorageNoLife) Size() uint64 {
	return 0
}

func (ss *SomeStorageNoLife) Flush() error {
	return nil
}

func (ss *SomeStorageNoLife) Close() error {
	return nil
}

func (ss *SomeStorageNoLife) CancelWrites(int64, int64) {
}

func (ss *SomeStorageNoLife) UUID() []uuid.UUID {
	return nil
}

func TestStorageEvents(t *testing.T) {
	ss := NewSomeStorage()

	var wg sync.WaitGroup
	wg.Add(1)
	ok := AddEventNotification(ss, "testing", func(event EventType, data EventData) EventReturnData {
		// Do something here
		assert.Equal(t, EventType("testing"), event)
		assert.Equal(t, "HELLO WORLD", data.(string))
		wg.Done()

		return "SOMETHING"
	})
	assert.True(t, ok)

	data := SendEvent(ss, "testing", EventData("HELLO WORLD"))
	assert.Equal(t, 1, len(data))
	assert.Equal(t, "SOMETHING", data[0].(string))

	wg.Wait()

	// Try doing it on something that doesn't support events

	ssnl := NewSomeStorageNoLife()

	ok = AddEventNotification(ssnl, "testing", func(from EventType, to EventData) EventReturnData {
		assert.Fail(t, "shouldn't happen")
		return nil
	})
	assert.False(t, ok)
	data = SendEvent(ssnl, "testing", nil)
	assert.Equal(t, 0, len(data))

}
