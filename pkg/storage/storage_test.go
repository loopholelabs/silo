package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
)

type SomeStorage struct {
	StorageProviderLifecycleState
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

func TestStorageLifecycle(t *testing.T) {
	ss := NewSomeStorage()

	var wg sync.WaitGroup
	wg.Add(1)
	ok := AddLifecycleNotification(ss, Lifecycle_migrating_to, func(from LifecycleState, to LifecycleState) {
		// Do something here
		assert.Equal(t, from, Lifecycle_none)
		assert.Equal(t, to, Lifecycle_migrating_to)
		wg.Done()
	})
	assert.True(t, ok)

	ok = SetLifecycleState(ss, Lifecycle_migrating_to)
	assert.True(t, ok)

	wg.Wait()

	state, ok := GetLifecycleState(ss)
	assert.True(t, ok)
	assert.Equal(t, Lifecycle_migrating_to, state)

	// Try doing it on something that doesn't support lifecycle

	ssnl := NewSomeStorageNoLife()

	ok = AddLifecycleNotification(ssnl, Lifecycle_migrating_to, func(from LifecycleState, to LifecycleState) {
		assert.Fail(t, "shouldn't happen")
	})
	assert.False(t, ok)
	ok = SetLifecycleState(ssnl, Lifecycle_migrating_to)
	assert.False(t, ok)
	_, ok = GetLifecycleState(ssnl)
	assert.False(t, ok)

}
