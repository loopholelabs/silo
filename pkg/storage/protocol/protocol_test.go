package protocol

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadAt(t *testing.T) {

	b := EncodeReadAt(12345, 10)

	off, length, err := DecodeReadAt(b)
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), off)
	assert.Equal(t, int32(10), length)

	// Make sure we can't decode silly things
	_, _, err = DecodeReadAt(nil)
	assert.Error(t, err)

	_, _, err = DecodeReadAt([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestReadAtResponse(t *testing.T) {
	rar := &ReadAtResponse{
		Bytes: 10,
		Error: nil,
		Data:  []byte{1, 2, 3, 4, 5},
	}

	b := EncodeReadAtResponse(rar)

	rar2, err := DecodeReadAtResponse(b)
	assert.NoError(t, err)
	assert.Equal(t, rar.Bytes, rar2.Bytes)
	assert.Equal(t, rar.Data, rar2.Data)
	assert.Equal(t, rar.Error, rar2.Error)

	// Make sure we can't decode silly things
	_, err = DecodeReadAtResponse(nil)
	assert.Error(t, err)

	_, err = DecodeReadAtResponse([]byte{
		99,
	})
	assert.Error(t, err)

	// Test encoding error
	be := EncodeReadAtResponse(&ReadAtResponse{Error: errors.New("Something")})

	rare, err := DecodeReadAtResponse(be)
	assert.NoError(t, err)
	assert.Error(t, rare.Error)

}

func TestWriteAt(t *testing.T) {

	b := EncodeWriteAt(12345, []byte{1, 2, 3, 4, 5})

	off, data, err := DecodeWriteAt(b)

	assert.NoError(t, err)
	assert.Equal(t, int64(12345), off)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, data)

	// Make sure we can't decode silly things
	_, _, err = DecodeWriteAt(nil)
	assert.Error(t, err)

	_, _, err = DecodeWriteAt([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestWriteAtResponse(t *testing.T) {
	war := &WriteAtResponse{
		Bytes: 10,
		Error: nil,
	}

	b := EncodeWriteAtResponse(war)

	war2, err := DecodeWriteAtResponse(b)
	assert.NoError(t, err)
	assert.Equal(t, war.Bytes, war2.Bytes)
	assert.Equal(t, war.Error, war2.Error)

	// Make sure we can't decode silly things
	_, err = DecodeWriteAtResponse(nil)
	assert.Error(t, err)

	_, err = DecodeWriteAtResponse([]byte{
		99,
	})
	assert.Error(t, err)

	// Test encoding error
	be := EncodeWriteAtResponse(&WriteAtResponse{Error: errors.New("Something")})

	rare, err := DecodeWriteAtResponse(be)
	assert.NoError(t, err)
	assert.Error(t, rare.Error)

}

func TestNeedAt(t *testing.T) {

	b := EncodeNeedAt(12345, 10)

	off, length, err := DecodeNeedAt(b)
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), off)
	assert.Equal(t, int32(10), length)

	// Make sure we can't decode silly things
	_, _, err = DecodeNeedAt(nil)
	assert.Error(t, err)

	_, _, err = DecodeNeedAt([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestDirtyList(t *testing.T) {

	blocks := []uint{1, 7, 100}
	b := EncodeDirtyList(blocks)

	blocks2, err := DecodeDirtyList(b)
	assert.NoError(t, err)
	assert.Equal(t, blocks, blocks2)

	// Make sure we can't decode silly things
	_, err = DecodeDirtyList(nil)
	assert.Error(t, err)

	_, err = DecodeDirtyList([]byte{
		99,
	})
	assert.Error(t, err)

}
