package protocol

const IDPickAny = 0

type Urgency int

const UrgencyNormal = Urgency(0)
const UrgencyUrgent = Urgency(1)

type Protocol interface {
	// Send a packet (Returns a transaction id)
	SendPacket(dev uint32, id uint32, data []byte, urgency Urgency) (uint32, error)

	// Wait for a response packet (Given specific transaction id)
	WaitForPacket(dev uint32, id uint32) ([]byte, error)

	// Wait for a specific command
	WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error)
}
