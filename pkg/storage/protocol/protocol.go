package protocol

import "io"

const COMMAND_REQUEST = byte(0)
const COMMAND_RESPONSE = byte(0x80)

const (
	COMMAND_READ_AT       = COMMAND_REQUEST | byte(1)
	COMMAND_WRITE_AT      = COMMAND_REQUEST | byte(2)
	COMMAND_NEED_AT       = COMMAND_REQUEST | byte(3)
	COMMAND_DONT_NEED_AT  = COMMAND_REQUEST | byte(4)
	COMMAND_DIRTY_LIST    = COMMAND_REQUEST | byte(5)
	COMMAND_DEV_INFO      = COMMAND_REQUEST | byte(6)
	COMMAND_EVENT         = COMMAND_REQUEST | byte(7)
	COMMAND_WRITE_AT_COMP = COMMAND_REQUEST | byte(8)
	COMMAND_HASHES        = COMMAND_REQUEST | byte(9)
)

const (
	COMMAND_READ_AT_RESPONSE      = COMMAND_RESPONSE | byte(1)
	COMMAND_READ_AT_RESPONSE_ERR  = COMMAND_RESPONSE | byte(2)
	COMMAND_WRITE_AT_RESPONSE     = COMMAND_RESPONSE | byte(3)
	COMMAND_WRITE_AT_RESPONSE_ERR = COMMAND_RESPONSE | byte(4)
	COMMAND_EVENT_RESPONSE        = COMMAND_RESPONSE | byte(5)
	COMMAND_HASHES_RESPONSE       = COMMAND_RESPONSE | byte(6)
)

const ID_PICK_ANY = 0

type Protocol interface {
	// Send a packet (Returns a transaction id)
	SendPacket(dev uint32, id uint32, data []byte) (uint32, error)

	// Send a packet using a callback to write the data
	SendPacketWriter(dev uint32, id uint32, length uint32, data func(w io.Writer) error) (uint32, error)

	// Wait for a response packet (Given specific transaction id)
	WaitForPacket(dev uint32, id uint32) ([]byte, error)

	// Wait for a specific command
	WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error)
}

func IsResponse(cmd byte) bool {
	return (cmd & COMMAND_RESPONSE) == COMMAND_RESPONSE
}
