package protocol

const COMMAND_READ_AT = byte(1)
const COMMAND_READ_AT_RESPONSE = byte(2)
const COMMAND_READ_AT_RESPONSE_ERR = byte(3)

const COMMAND_WRITE_AT = byte(0x10)
const COMMAND_WRITE_AT_RESPONSE = byte(0x11)
const COMMAND_WRITE_AT_RESPONSE_ERR = byte(0x12)

const ID_PICK_ANY = 0

type Protocol interface {
	// Send a packet (Returns a transaction id)
	SendPacket(dev uint32, id uint32, data []byte) (uint32, error)

	// Wait for a response packet (Given specific transaction id)
	WaitForPacket(dev uint32, id uint32) ([]byte, error)

	// Wait for a specific command
	WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error)
}
