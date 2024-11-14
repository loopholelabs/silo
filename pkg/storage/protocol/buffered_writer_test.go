package protocol_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/stretchr/testify/assert"
)

type loggedWriter struct {
	lock   sync.Mutex
	writes []int
	err    chan error
}

func (lw *loggedWriter) GetWrites() []int {
	lw.lock.Lock()
	w := make([]int, len(lw.writes))
	copy(w, lw.writes)
	lw.lock.Unlock()
	return w
}

func (lw *loggedWriter) Write(buffer []byte) (int, error) {
	// Return an error if one has been set
	select {
	case err := <-lw.err:
		return 0, err
	default:
	}
	// Just log it
	lw.lock.Lock()
	lw.writes = append(lw.writes, len(buffer))
	lw.lock.Unlock()
	return len(buffer), nil
}

func newLoggedWriter() *loggedWriter {
	return &loggedWriter{
		writes: make([]int, 0),
		err:    make(chan error, 1),
	}
}

func TestBufferedWriter(t *testing.T) {
	w := newLoggedWriter()
	bw := protocol.NewBufferedWriter(w, &protocol.BufferedWriterConfig{Timeout: 1 * time.Minute, DelayTimer: true, MaxLength: 100})
	for i := 0; i < 10; i++ {
		data := make([]byte, 50)
		_, err := bw.Write(data)
		assert.NoError(t, err)
	}

	err := bw.Flush()
	assert.NoError(t, err)

	assert.Equal(t, []int{100, 100, 100, 100, 100}, w.GetWrites())
}

func TestBufferedWriterTimed(t *testing.T) {
	w := newLoggedWriter()
	bw := protocol.NewBufferedWriter(w, &protocol.BufferedWriterConfig{Timeout: 100 * time.Millisecond, DelayTimer: true, MaxLength: 1000000})
	for i := 0; i < 10; i++ {
		data := make([]byte, 50)
		_, err := bw.Write(data)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < 10; i++ {
		data := make([]byte, 50)
		_, err := bw.Write(data)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}

	err := bw.Flush()
	assert.NoError(t, err)

	assert.Equal(t, []int{500, 500}, w.GetWrites())
}

func TestBufferedWriterNow(t *testing.T) {
	w := newLoggedWriter()
	bw := protocol.NewBufferedWriter(w, &protocol.BufferedWriterConfig{Timeout: 100 * time.Second, DelayTimer: true, MaxLength: 1000000})
	data := make([]byte, 50)
	for i := 0; i < 10; i++ {
		_, err := bw.Write(data)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}

	_, err := bw.WriteNow(data) // This will do a flush
	assert.NoError(t, err)

	assert.Equal(t, []int{550}, w.GetWrites())
}

func TestBufferedWriterError(t *testing.T) {
	w := newLoggedWriter()
	bw := protocol.NewBufferedWriter(w, &protocol.BufferedWriterConfig{Timeout: 100 * time.Millisecond, DelayTimer: true, MaxLength: 1000000})
	data := make([]byte, 50)
	for i := 0; i < 10; i++ {
		_, err := bw.Write(data)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}
	someError := errors.New("Hello")
	w.err <- someError

	// Make sure a write happens here, which has an error
	time.Sleep(200 * time.Millisecond)

	// The write failed, so it won't have been recorded
	assert.Equal(t, []int{}, w.GetWrites())

	// Make sure any write results in the error now...
	_, err := bw.Write(data)
	assert.ErrorIs(t, someError, err)
}

func TestBufferedWriterDelayTimerFalse(t *testing.T) {
	w := newLoggedWriter()
	bw := protocol.NewBufferedWriter(w, &protocol.BufferedWriterConfig{Timeout: 100 * time.Millisecond, DelayTimer: false, MaxLength: 1000000})
	data := make([]byte, 50)
	for i := 0; i < 10; i++ {
		_, err := bw.Write(data)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}
	// Should have written something by now, since the timer is not reset...
	assert.Greater(t, len(w.GetWrites()), 0)
}

func TestBufferedWriterDelayTimerTrue(t *testing.T) {
	w := newLoggedWriter()
	bw := protocol.NewBufferedWriter(w, &protocol.BufferedWriterConfig{Timeout: 100 * time.Millisecond, DelayTimer: true, MaxLength: 1000000})
	data := make([]byte, 50)
	for i := 0; i < 10; i++ {
		_, err := bw.Write(data)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}
	// Nothing should have been written, since the timer gets delayed each time
	assert.Equal(t, 0, len(w.GetWrites()))
}
