package packets

import (
	"crypto/sha256"
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

func TestDontNeedAt(t *testing.T) {

	b := EncodeDontNeedAt(12345, 10)

	off, length, err := DecodeDontNeedAt(b)
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), off)
	assert.Equal(t, int32(10), length)

	// Make sure we can't decode silly things
	_, _, err = DecodeDontNeedAt(nil)
	assert.Error(t, err)

	_, _, err = DecodeDontNeedAt([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestDirtyList(t *testing.T) {

	blocks := []uint{1, 7, 100}
	b := EncodeDirtyList(4096, blocks)

	bs, blocks2, err := DecodeDirtyList(b)
	assert.NoError(t, err)
	assert.Equal(t, 4096, bs)
	assert.Equal(t, blocks, blocks2)

	// Make sure we can't decode silly things
	_, _, err = DecodeDirtyList(nil)
	assert.Error(t, err)

	_, _, err = DecodeDirtyList([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestDevInfo(t *testing.T) {
	b := EncodeDevInfo(&DevInfo{Size: 12345, BlockSize: 55, Name: "hello", Schema: "1234"})

	di, err := DecodeDevInfo(b)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12345), di.Size)
	assert.Equal(t, uint32(55), di.BlockSize)
	assert.Equal(t, "hello", di.Name)
	assert.Equal(t, "1234", di.Schema)

	// Make sure we can't decode silly things
	_, err = DecodeDevInfo(nil)
	assert.Error(t, err)

	_, err = DecodeDevInfo([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestEvent(t *testing.T) {
	b := EncodeEvent(&Event{Type: EventCompleted})

	e, err := DecodeEvent(b)
	assert.NoError(t, err)
	assert.Equal(t, EventCompleted, e.Type)

	// Make sure we can't decode silly things
	_, err = DecodeEvent(nil)
	assert.Error(t, err)

	_, err = DecodeEvent([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestCustomEvent(t *testing.T) {
	b := EncodeEvent(&Event{Type: EventCustom, CustomType: 9, CustomPayload: []byte{1, 2, 3}})

	e, err := DecodeEvent(b)
	assert.NoError(t, err)
	assert.Equal(t, EventCustom, e.Type)
	assert.Equal(t, byte(9), e.CustomType)
	assert.Equal(t, []byte{1, 2, 3}, e.CustomPayload)

}

func TestEventResponse(t *testing.T) {
	b := EncodeEventResponse()

	err := DecodeEventResponse(b)
	assert.NoError(t, err)

	// Make sure we can't decode silly things
	err = DecodeEventResponse(nil)
	assert.Error(t, err)

	err = DecodeEventResponse([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestWriteAtComp(t *testing.T) {

	buff := []byte{26,
		0, 0, 0, 0,
		1, 2, 0, 0, 0,
		4, 4, 4, 4, 4,
		0, 0, 9, 9, 5, 23,
		8, 8, 8, 8, 8, 8, 8, 8,
		0, 7}

	for _, compType := range []CompressionType{CompressionTypeRLE, CompressionTypeGzip, CompressionTypeZeroes} {
		b, err := EncodeWriteAtComp(compType, 12345, buff)
		assert.NoError(t, err)

		off, data, err := DecodeWriteAtComp(b)
		assert.NoError(t, err)

		assert.Equal(t, int64(12345), off)
		assert.Equal(t, buff, data)
	}
}

func TestWriteAtWithMap(t *testing.T) {

	b := EncodeWriteAtWithMap(12345, []byte{1, 2, 3, 4, 5}, map[uint64]uint64{
		1: 7,
		5: 80,
	})

	off, data, idmap, err := DecodeWriteAtWithMap(b)

	assert.NoError(t, err)
	assert.Equal(t, int64(12345), off)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, data)
	assert.Equal(t, map[uint64]uint64{1: 7, 5: 80}, idmap)

	// Make sure we can't decode silly things
	_, _, err = DecodeWriteAt(nil)
	assert.Error(t, err)

	_, _, err = DecodeWriteAt([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestRemoveFromMap(t *testing.T) {

	blocks := []uint64{1, 7, 100}
	b := EncodeRemoveFromMap(blocks)

	blocks2, err := DecodeRemoveFromMap(b)
	assert.NoError(t, err)
	assert.Equal(t, blocks, blocks2)

	// Make sure we can't decode silly things
	_, err = DecodeRemoveFromMap(nil)
	assert.Error(t, err)

	_, err = DecodeRemoveFromMap([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestRemoveDev(t *testing.T) {

	b := EncodeRemoveDev()

	err := DecodeRemoveDev(b)
	assert.NoError(t, err)

	// Make sure we can't decode silly things
	err = DecodeRemoveDev(nil)
	assert.Error(t, err)

	err = DecodeRemoveDev([]byte{
		99,
	})
	assert.Error(t, err)

}

func TestHashes(t *testing.T) {

	hashes := map[uint][32]byte{
		1: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
		2: {2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
	}
	b := EncodeHashes(hashes)

	hashes2, err := DecodeHashes(b)
	assert.NoError(t, err)
	assert.Equal(t, len(hashes), len(hashes2))
	assert.Equal(t, hashes[1], hashes2[1])
	assert.Equal(t, hashes[2], hashes2[2])

}

func TestAlternateSources(t *testing.T) {

	sources := []AlternateSource{
		{
			Offset:   0,
			Length:   100,
			Hash:     [sha256.Size]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
			Location: "somewhere",
		},
	}
	b := EncodeAlternateSources(sources)

	sources2, err := DecodeAlternateSources(b)
	assert.NoError(t, err)
	assert.Equal(t, len(sources), len(sources2))

	assert.Equal(t, sources[0].Offset, sources2[0].Offset)
	assert.Equal(t, sources[0].Length, sources2[0].Length)
	assert.Equal(t, sources[0].Hash, sources2[0].Hash)
	assert.Equal(t, sources[0].Location, sources2[0].Location)

}

func TestDeviceGroupInfo(t *testing.T) {
	dgi := &DeviceGroupInfo{
		Devices: map[int]*DevInfo{
			0: {Size: 100, BlockSize: 1, Name: "a-hello", Schema: "a-1234"},
			1: {Size: 200, BlockSize: 2, Name: "b-hello", Schema: "b-1234"},
			3: {Size: 300, BlockSize: 3, Name: "c-hello", Schema: "c-1234"},
		},
	}
	b := EncodeDeviceGroupInfo(dgi)

	dgi2, err := DecodeDeviceGroupInfo(b)
	assert.NoError(t, err)

	// Check that dgi and dgi2 are the same...
	assert.Equal(t, len(dgi.Devices), len(dgi2.Devices))

	for index, di := range dgi.Devices {
		di2, ok := dgi2.Devices[index]
		assert.True(t, ok)
		assert.Equal(t, di.Size, di2.Size)
		assert.Equal(t, di.BlockSize, di2.BlockSize)
		assert.Equal(t, di.Name, di2.Name)
		assert.Equal(t, di.Schema, di2.Schema)
	}
}

func TestYouAlreadyHave(t *testing.T) {

	blocks := []uint32{0, 66, 180, 3}
	blockSize := uint64(500000)

	b := EncodeYouAlreadyHave(blockSize, blocks)

	blockSize2, blocks2, err := DecodeYouAlreadyHave(b)
	assert.NoError(t, err)
	assert.Equal(t, len(blocks), len(blocks2))
	assert.Equal(t, blockSize, blockSize2)
	assert.Equal(t, blocks, blocks2)

}
