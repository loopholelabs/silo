package packets

type EventType byte

const EventPreLock = EventType(0)
const EventPostLock = EventType(1)
const EventPreUnlock = EventType(2)
const EventPostUnlock = EventType(3)
const EventCompleted = EventType(4)
const EventCustom = EventType(5)

var EventsByType = map[EventType]string{
	EventPreLock:    "PreLock",
	EventPostLock:   "PostLock",
	EventPreUnlock:  "PreUnlock",
	EventPostUnlock: "PostUnlock",
	EventCompleted:  "Completed",
	EventCustom:     "Custom",
}

type Event struct {
	Type          EventType
	CustomType    byte
	CustomPayload []byte
}

func EncodeEvent(e *Event) []byte {
	if e.Type == EventCustom {
		buff := make([]byte, 1+1+1+len(e.CustomPayload))
		buff[0] = CommandEvent
		buff[1] = byte(e.Type)
		buff[2] = e.CustomType
		copy(buff[3:], e.CustomPayload)
		return buff
	}
	buff := make([]byte, 1+1)
	buff[0] = CommandEvent
	buff[1] = byte(e.Type)
	return buff
}

func DecodeEvent(buff []byte) (*Event, error) {
	if len(buff) < 2 || buff[0] != CommandEvent {
		return nil, ErrInvalidPacket
	}
	e := &Event{Type: EventType(buff[1])}
	if e.Type == EventCustom {
		// Decode the other bits
		if len(buff) < 3 {
			return nil, ErrInvalidPacket
		}
		e.CustomType = buff[2]
		e.CustomPayload = buff[3:]
	}
	return e, nil
}

func EncodeEventResponse() []byte {
	buff := make([]byte, 1)
	buff[0] = CommandEventResponse
	return buff
}

func DecodeEventResponse(buff []byte) error {
	if len(buff) < 1 || buff[0] != CommandEventResponse {
		return ErrInvalidPacket
	}
	return nil
}
