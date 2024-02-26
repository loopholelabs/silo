package protocol

import (
	"errors"
)

type EventType byte

const EventPreLock = EventType(0)
const EventPostLock = EventType(1)
const EventPreUnlock = EventType(2)
const EventPostUnlock = EventType(3)
const EventCompleted = EventType(4)

var EventsByType = map[EventType]string{
	EventPreLock:    "PreLock",
	EventPostLock:   "PostLock",
	EventPreUnlock:  "PreUnlock",
	EventPostUnlock: "PostUnlock",
	EventCompleted:  "Completed",
}

type Event struct {
	Type EventType
}

func EncodeEvent(e *Event) []byte {
	buff := make([]byte, 1+1)
	buff[0] = COMMAND_EVENT
	buff[1] = byte(e.Type)
	return buff
}

func DecodeEvent(buff []byte) (*Event, error) {
	if buff == nil || len(buff) < 2 || buff[0] != COMMAND_EVENT {
		return nil, errors.New("Invalid packet")
	}
	return &Event{Type: EventType(buff[1])}, nil
}

func EncodeEventResponse() []byte {
	buff := make([]byte, 1)
	buff[0] = COMMAND_EVENT_RESPONSE
	return buff
}

func DecodeEventResponse(buff []byte) error {
	if buff == nil || len(buff) < 1 || buff[0] != COMMAND_EVENT_RESPONSE {
		return errors.New("Invalid packet")
	}
	return nil
}
