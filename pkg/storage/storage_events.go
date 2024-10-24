package storage

import "sync"

/**
 * Events are an optional addition to StorageProvider.
 *
 * To support events, a StorageProvider can simply embed StorageProviderWithEvents, or do it's own impl of StorageProviderWithEventsIfc
 *
 * StorageProviders should also relay events to any StorageProviders they wrap.
 */

type StorageProviderWithEventsIfc interface {
	StorageProvider
	SendSiloEvent(EventType, EventData) []EventReturnData
	AddSiloEventNotification(EventType, EventCallback)
}

// Try to send an event for a given StorageProvider
func SendSiloEvent(s StorageProvider, event_type EventType, event_data EventData) []EventReturnData {
	lcsp, ok := s.(StorageProviderWithEventsIfc)
	if ok {
		return lcsp.SendSiloEvent(event_type, event_data)
	}
	return nil
}

// Try to add an event notification on a StorageProvider
func AddSiloEventNotification(s StorageProvider, state EventType, callback EventCallback) bool {
	lcsp, ok := s.(StorageProviderWithEventsIfc)
	if ok {
		lcsp.AddSiloEventNotification(state, callback)
	}
	return ok
}

/**
 * A StorageProvider can simply embed StorageProviderWithEvents to support events
 *
 */
type EventType string
type EventData interface{}
type EventReturnData interface{}

type EventCallback func(event EventType, data EventData) EventReturnData

type StorageProviderWithEvents struct {
	lock      sync.Mutex
	callbacks map[EventType][]EventCallback
}

// Send an event, and notify any callbacks
func (spl *StorageProviderWithEvents) SendSiloEvent(event_type EventType, event_data EventData) []EventReturnData {
	spl.lock.Lock()
	defer spl.lock.Unlock()
	if spl.callbacks == nil {
		return nil
	}
	cbs, ok := spl.callbacks[event_type]
	if ok {
		rets := make([]EventReturnData, 0)
		for _, cb := range cbs {
			rets = append(rets, cb(event_type, event_data))
		}
		return rets
	}
	return nil
}

// Add a new callback for the given state.
func (spl *StorageProviderWithEvents) AddSiloEventNotification(event_type EventType, callback EventCallback) {
	spl.lock.Lock()
	defer spl.lock.Unlock()
	if spl.callbacks == nil {
		spl.callbacks = make(map[EventType][]EventCallback)
	}
	_, ok := spl.callbacks[event_type]
	if ok {
		spl.callbacks[event_type] = append(spl.callbacks[event_type], callback)
	} else {
		spl.callbacks[event_type] = []EventCallback{callback}
	}
}
