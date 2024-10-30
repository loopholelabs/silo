package modules

import (
	"errors"
	"fmt"

	"github.com/loopholelabs/silo/pkg/storage"
)

type Raid struct {
	storage.StorageProviderWithEvents
	prov []storage.StorageProvider
}

// Relay events to embedded StorageProvider
func (r *Raid) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := r.StorageProviderWithEvents.SendSiloEvent(eventType, eventData)
	for _, pr := range r.prov {
		data = append(data, storage.SendSiloEvent(pr, eventType, eventData)...)
	}
	return data
}

func NewRaid(prov []storage.StorageProvider) (*Raid, error) {
	if len(prov) == 0 {
		return nil, errors.New("need at least one provider")
	}
	return &Raid{
		prov: prov,
	}, nil
}

func (r *Raid) ReadAt(buffer []byte, offset int64) (int, error) {
	var err error
	var count int
	for index, p := range r.prov {
		if index == 0 {
			n, e := p.ReadAt(buffer, offset)
			err = e
			count = n
		} else {
			buffer2 := make([]byte, len(buffer))
			n, e := p.ReadAt(buffer2, offset)

			if e != err || n != count {
				// RAID ERROR!
				return 0, fmt.Errorf("raid corruption on ReadAt (%d/%d,%v/%d)", n, count, e, err)
			}
			// Check the contents match
			for c := 0; c < count; c++ {
				if buffer[c] != buffer2[c] {
					// RAID ERROR!
					return 0, errors.New("raid corruption on ReadAt contents")
				}
			}
		}
	}
	return count, err
}

func (r *Raid) WriteAt(buffer []byte, offset int64) (int, error) {
	var err error
	var count int
	for index, p := range r.prov {
		n, e := p.WriteAt(buffer, offset)
		// Make sure they all agree...
		if index == 0 {
			err = e
			count = n
		} else if e != err || n != count {
			// RAID ERROR!
			return 0, fmt.Errorf("raid corruption on WriteAt (%d/%d,%v/%d)", n, count, e, err)
		}

	}
	return count, err
}

func (r *Raid) Flush() error {
	var err error
	for _, p := range r.prov {
		e := p.Flush()
		if err == nil && e != nil {
			err = e // Pick one of the errors
		}
	}
	return err
}

func (r *Raid) Size() uint64 {
	return r.prov[0].Size()
}

func (r *Raid) Close() error {
	var err error
	for _, p := range r.prov {
		e := p.Close()
		if err == nil && e != nil {
			err = e // Pick one of the errors
		}
	}
	return err
}

func (r *Raid) CancelWrites(_ int64, _ int64) {
	// TODO: Implement
}
