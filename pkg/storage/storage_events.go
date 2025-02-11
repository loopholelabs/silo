package storage

import "sync"

const EventTypeSources = EventType("sources")
const EventTypeCowGetBlocks = EventType("cow.get.blocks")
const EventTypeAvailable = EventType("available")

const EventSyncStop = EventType("sync.stop")
const EventSyncStart = EventType("sync.start")
const EventSyncStatus = EventType("sync.status")
const EventSyncRunning = EventType("sync.running")

/**
 * Events are an optional addition to StorageProvider.
 *
 * To support events, a StorageProvider can simply embed StorageProviderWithEvents, or do it's own impl of StorageProviderWithEventsIfc
 *
 * StorageProviders should also relay events to any StorageProviders they wrap.
 */

type ProviderWithEventsIfc interface {
	Provider
	SendSiloEvent(EventType, EventData) []EventReturnData
	AddSiloEventNotification(EventType, EventCallback)
}

// Try to send an event for a given StorageProvider
func SendSiloEvent(s Provider, eventType EventType, eventData EventData) []EventReturnData {
	lcsp, ok := s.(ProviderWithEventsIfc)
	if ok {
		return lcsp.SendSiloEvent(eventType, eventData)
	}
	return nil
}

// Try to add an event notification on a StorageProvider
func AddSiloEventNotification(s Provider, state EventType, callback EventCallback) bool {
	lcsp, ok := s.(ProviderWithEventsIfc)
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

type ProviderWithEvents struct {
	lock      sync.Mutex
	callbacks map[EventType][]EventCallback
}

// Send an event, and notify any callbacks
func (spl *ProviderWithEvents) SendSiloEvent(eventType EventType, eventData EventData) []EventReturnData {
	spl.lock.Lock()
	defer spl.lock.Unlock()
	if spl.callbacks == nil {
		return nil
	}
	cbs, ok := spl.callbacks[eventType]
	if ok {
		rets := make([]EventReturnData, 0)
		for _, cb := range cbs {
			rets = append(rets, cb(eventType, eventData))
		}
		return rets
	}
	return nil
}

// Add a new callback for the given state.
func (spl *ProviderWithEvents) AddSiloEventNotification(eventType EventType, callback EventCallback) {
	spl.lock.Lock()
	defer spl.lock.Unlock()
	if spl.callbacks == nil {
		spl.callbacks = make(map[EventType][]EventCallback)
	}
	_, ok := spl.callbacks[eventType]
	if ok {
		spl.callbacks[eventType] = append(spl.callbacks[eventType], callback)
	} else {
		spl.callbacks[eventType] = []EventCallback{callback}
	}
}
