package protocol

import (
	"bytes"
	"io"
	"sync"
	"time"
)

type WriterWithUrgent interface {
	io.Writer
	WriteNow([]byte) (int, error)
}

type BufferedWriterConfig struct {
	Timeout    time.Duration // Delay a flush by this amount
	DelayTimer bool          // If true, the delayed flush gets rescheduled each write
	MaxLength  int           // Max data to buffer. If more, we will flush on write
}

type BufferedWriter struct {
	w          io.Writer
	lock       sync.Mutex
	buffer     bytes.Buffer
	timeout    time.Duration
	maxLength  int
	delayTimer bool
	flushTimer *time.Timer
	flushErr   chan error
}

/**
 *
 *
 */
func NewBufferedWriter(w io.Writer, config *BufferedWriterConfig) *BufferedWriter {
	return &BufferedWriter{
		w:          w,
		timeout:    config.Timeout,
		delayTimer: config.DelayTimer,
		maxLength:  config.MaxLength,
		flushErr:   make(chan error, 1),
	}
}

// Write with a forced flush
func (bw *BufferedWriter) WriteNow(buffer []byte) (int, error) {
	// See if there's any error to report from delayed flushing.
	select {
	case err := <-bw.flushErr:
		return 0, err
	default:
	}

	bw.lock.Lock()
	bw.buffer.Write(buffer)

	// Flush the data now, and clear the buffer
	data := bw.buffer.Bytes()
	bw.buffer.Reset()
	bw.lock.Unlock()

	n, err := bw.w.Write(data)
	return n, err
}

// Write with a delayed flush
func (bw *BufferedWriter) Write(buffer []byte) (int, error) {
	// See if there's any error to report from delayed flushing.
	select {
	case err := <-bw.flushErr:
		return 0, err
	default:
	}

	bw.lock.Lock()
	if bw.flushTimer == nil || bw.delayTimer {
		if bw.delayTimer && bw.flushTimer != nil {
			// Cancel any existing timer here...
			bw.flushTimer.Stop()
			bw.flushTimer = nil
		}

		// If we don't currently have a timer setup, set one up here.
		if bw.flushTimer == nil {
			// Schedule a flush...
			bw.flushTimer = time.AfterFunc(bw.timeout, func() {
				bw.lock.Lock()
				bw.flushTimer = nil
				// Nothing to do anyway
				if bw.buffer.Len() == 0 {
					bw.lock.Unlock()
					return
				}
				data := bw.buffer.Bytes()

				_, err := bw.w.Write(data)
				bw.buffer.Reset()
				bw.lock.Unlock()
				if err != nil {
					// Put the error in flushErr
					select {
					case bw.flushErr <- err:
					default:
					}
				}
			})
		}
	}
	bw.buffer.Write(buffer)
	if bw.buffer.Len() >= bw.maxLength {
		// Flush the data now, and clear the buffer
		data := bw.buffer.Bytes()
		n, err := bw.w.Write(data)
		bw.buffer.Reset()
		bw.lock.Unlock()
		return n, err
	}
	bw.lock.Unlock()

	// Report no error
	return len(buffer), nil
}

// Flush all data
func (bw *BufferedWriter) Flush() error {
	// See if there's any error to report from delayed flushing.
	select {
	case err := <-bw.flushErr:
		return err
	default:
	}

	bw.lock.Lock()
	// Clear any existing timed flush.
	if bw.flushTimer != nil {
		bw.flushTimer.Stop()
		bw.flushTimer = nil
	}
	// Nothing to do anyway
	if bw.buffer.Len() == 0 {
		bw.lock.Unlock()
		return nil
	}
	data := bw.buffer.Bytes()
	bw.buffer.Reset()
	bw.lock.Unlock()

	_, err := bw.w.Write(data)
	return err
}
