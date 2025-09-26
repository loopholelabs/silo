package memory

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// GetPids gets all children PIDs from a root PID. The process is assumed to be STOPPED.
func GetPids(pid int) ([]int, error) {
	pids := make([]int, 0)
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			// Then cat /proc/PID/task/TID/children
			children, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", pid, e.Name()))
			if err != nil {
				return nil, err
			}
			cpids := strings.Fields(string(children))
			for _, c := range cpids {
				cpid, err := strconv.ParseInt(c, 10, 64)
				if err != nil {
					return nil, err
				}
				pids = append(pids, int(cpid))
			}
		}
	}
	return pids, nil
}
