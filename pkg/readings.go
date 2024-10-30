package pkg

import (
	"sync"
	"time"
)

type entry struct {
	time  time.Time
	value float64
}

type Readings struct {
	values     []*entry
	valuesLock sync.Mutex
}

func NewReadings() *Readings {
	return &Readings{}
}

func (r *Readings) Add(v float64) {
	r.valuesLock.Lock()
	defer r.valuesLock.Unlock()
	r.values = append(r.values, &entry{
		time:  time.Now(),
		value: v,
	})
}

func (r *Readings) GetAverage(d time.Duration) float64 {
	r.valuesLock.Lock()
	defer r.valuesLock.Unlock()
	ctime := time.Now().Add(-d)
	num := 0
	total := float64(0)
	for _, e := range r.values {
		if e.time.After(ctime) {
			total += e.value
			num++
		}
	}
	if num == 0 {
		return 0
	}
	return total / float64(num)
}
