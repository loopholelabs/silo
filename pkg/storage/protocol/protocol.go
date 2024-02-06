package protocol

const COMMAND_REQUEST = byte(0)
const COMMAND_RESPONSE = byte(0x80)

const COMMAND_READ_AT = COMMAND_REQUEST | byte(1)
const COMMAND_WRITE_AT = COMMAND_REQUEST | byte(2)
const COMMAND_NEED_AT = COMMAND_REQUEST | byte(3)
const COMMAND_DIRTY_LIST = COMMAND_REQUEST | byte(4)

const COMMAND_READ_AT_RESPONSE = COMMAND_RESPONSE | byte(1)
const COMMAND_READ_AT_RESPONSE_ERR = COMMAND_RESPONSE | byte(2)
const COMMAND_WRITE_AT_RESPONSE = COMMAND_RESPONSE | byte(3)
const COMMAND_WRITE_AT_RESPONSE_ERR = COMMAND_RESPONSE | byte(4)

const ID_PICK_ANY = 0

type Protocol interface {
	// Send a packet (Returns a transaction id)
	SendPacket(dev uint32, id uint32, data []byte) (uint32, error)

	// Wait for a response packet (Given specific transaction id)
	WaitForPacket(dev uint32, id uint32) ([]byte, error)

	// Wait for a specific command
	WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error)
}

func IsResponse(cmd byte) bool {
	return (cmd & COMMAND_RESPONSE) == COMMAND_RESPONSE
}
