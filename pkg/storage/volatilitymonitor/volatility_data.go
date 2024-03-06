package volatilitymonitor

import "time"

type volatilityData struct {
	log []int64
}

func (bd *volatilityData) Add(expiry time.Duration) {
	n := time.Now().UnixNano()
	for i := 0; i < len(bd.log); i++ {
		if bd.log[i] < n-int64(expiry) {
			bd.log[i] = n
			return
		}
	}
	bd.log = append(bd.log, n)
}

func (bd *volatilityData) Count(expiry time.Duration) int {
	n := time.Now().UnixNano()
	count := 0
	for i := 0; i < len(bd.log); i++ {
		if bd.log[i] >= n-int64(expiry) {
			count++
		}
	}
	return count
}

func (bd *volatilityData) Clean(expiry time.Duration) {
	n := time.Now().UnixNano()
	newlog := make([]int64, 0)
	for i := 0; i < len(bd.log); i++ {
		if bd.log[i] >= n-int64(expiry) {
			newlog = append(newlog, bd.log[i])
		}
	}
	bd.log = newlog
}
