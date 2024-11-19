package testutils

import (
	"bytes"
	"sync"
)

type SafeWriteBuffer struct {
	bufferLock sync.Mutex
	buffer     bytes.Buffer
}

func (swb *SafeWriteBuffer) Write(p []byte) (n int, err error) {
	swb.bufferLock.Lock()
	defer swb.bufferLock.Unlock()
	return swb.buffer.Write(p)
}

func (swb *SafeWriteBuffer) Bytes() []byte {
	swb.bufferLock.Lock()
	defer swb.bufferLock.Unlock()
	return swb.buffer.Bytes()
}

func (swb *SafeWriteBuffer) Len() int {
	swb.bufferLock.Lock()
	defer swb.bufferLock.Unlock()
	return swb.buffer.Len()
}
