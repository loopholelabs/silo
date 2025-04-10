package storage

import (
	"errors"
	"io"
)

// Implements io.ReadSeeker
type ReadSeeker struct {
	prov   Provider
	offset int64
}

func NewReadSeeker(p Provider) *ReadSeeker {
	return &ReadSeeker{
		prov:   p,
		offset: 0,
	}
}

func (rs *ReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		rs.offset = offset
	case io.SeekEnd:
		rs.offset = int64(rs.prov.Size()) + offset
	case io.SeekCurrent:
		rs.offset = rs.offset + offset
	}
	if offset > int64(rs.prov.Size()) || offset < 0 {
		return 0, errors.New("Seek outside file")
	}
	return rs.offset, nil
}

func (rs *ReadSeeker) Read(p []byte) (int, error) {
	return rs.prov.ReadAt(p, rs.offset)
}
