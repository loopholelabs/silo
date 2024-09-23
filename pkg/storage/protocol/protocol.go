package protocol

import "io"

const ID_PICK_ANY = 0

type Protocol interface {
	// Send a packet (Returns a transaction id)
	SendPacket(dev uint32, id uint32, data []byte) (uint32, error)

	// Send a packet using a callback to write the data
	SendPacketWriter(dev uint32, id uint32, length uint32, header []byte, data func(w io.Writer) error) (uint32, error)

	// Wait for a response packet (Given specific transaction id)
	WaitForPacket(dev uint32, id uint32) ([]byte, error)

	// Wait for a specific command
	WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error)
}
