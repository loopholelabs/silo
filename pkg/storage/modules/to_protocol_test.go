package modules

import (
	"crypto/rand"
	"errors"
	"testing"

	"github.com/loopholelabs/silo/pkg/storage/protocol"
	"github.com/stretchr/testify/assert"
)

type MockPro struct {
	sendPackets  chan *sendPacketInfo
	waitPackets  chan []byte
	waitCommands chan []byte
}

func NewMockPro() *MockPro {
	return &MockPro{
		sendPackets:  make(chan *sendPacketInfo, 8),
		waitPackets:  make(chan []byte, 8),
		waitCommands: make(chan []byte, 8),
	}
}

type sendPacketInfo struct {
	dev  uint32
	id   uint32
	data []byte
}

func (p *MockPro) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	mock_id := uint32(999)
	p.sendPackets <- &sendPacketInfo{
		dev:  dev,
		id:   mock_id,
		data: data,
	}
	return id, nil
}

func (p *MockPro) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	return <-p.waitPackets, nil
}

func (p *MockPro) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	return 123, <-p.waitCommands, nil
}

func TestToProtocolWriteAt(t *testing.T) {
	pro := NewMockPro()

	toproto := NewToProtocol(1024*1024, 123, pro)

	// Setup a mock response
	pro.waitPackets <- protocol.EncodeWriteAtResponse(&protocol.WriteAtResponse{
		Bytes: 1024,
		Error: nil,
	})

	data := make([]byte, 1024)
	rand.Read(data)
	n, err := toproto.WriteAt(data, 17)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	pack := <-pro.sendPackets

	assert.Equal(t, uint32(123), pack.dev)
	assert.Equal(t, uint32(999), pack.id)
	offset, data2, err := protocol.DecodeWriteAt(pack.data)
	assert.NoError(t, err)
	assert.Equal(t, int64(17), offset)
	assert.Equal(t, data, data2)
}

func TestToProtocolWriteAtError(t *testing.T) {
	pro := NewMockPro()

	toproto := NewToProtocol(1024*1024, 123, pro)

	// Setup a mock response
	pro.waitPackets <- protocol.EncodeWriteAtResponse(&protocol.WriteAtResponse{
		Bytes: 0,
		Error: errors.New("Something"),
	})

	buff := make([]byte, 1024)
	_, err := toproto.WriteAt(buff, 17)
	assert.Error(t, err)
}

func TestToProtocolReadAt(t *testing.T) {
	pro := NewMockPro()

	toproto := NewToProtocol(1024*1024, 123, pro)

	data := make([]byte, 1024)
	rand.Read(data)

	// Setup a mock response
	pro.waitPackets <- protocol.EncodeReadAtResponse(&protocol.ReadAtResponse{
		Bytes: 1024,
		Error: nil,
		Data:  data,
	})

	buff := make([]byte, 1024)
	n, err := toproto.ReadAt(buff, 17)
	assert.NoError(t, err)
	assert.Equal(t, len(buff), n)
	assert.Equal(t, data, buff)

	pack := <-pro.sendPackets

	assert.Equal(t, uint32(123), pack.dev)
	assert.Equal(t, uint32(999), pack.id)
	offset, length, err := protocol.DecodeReadAt(pack.data)
	assert.NoError(t, err)
	assert.Equal(t, int64(17), offset)
	assert.Equal(t, int32(1024), length)
}

func TestToProtocolReadAtError(t *testing.T) {
	pro := NewMockPro()

	toproto := NewToProtocol(1024*1024, 123, pro)

	// Setup a mock response
	pro.waitPackets <- protocol.EncodeReadAtResponse(&protocol.ReadAtResponse{
		Bytes: 0,
		Error: errors.New("Something"),
		Data:  nil,
	})

	buff := make([]byte, 1024)
	_, err := toproto.ReadAt(buff, 17)
	assert.Error(t, err)
}
