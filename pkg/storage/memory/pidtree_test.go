package memory

import (
	"os"
	"os/exec"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPidtree(t *testing.T) {
	pid := os.Getpid()

	// Get a list of children
	pids1, err := GetPids(pid)
	assert.NoError(t, err)

	// Start something long lived
	cmds := make([]*exec.Cmd, 4)

	for i := range cmds {
		cmds[i] = exec.Command("cat", "/dev/zero")

		err = cmds[i].Start()
		assert.NoError(t, err)
	}

	// Now we test the pid tree, make sure it contains what we expect
	pids2, err := GetPids(pid)
	assert.NoError(t, err)
	assert.Greater(t, len(pids2), len(pids1))

	// Check the children are present
	for i := range cmds {
		assert.True(t, slices.Contains(pids2, cmds[i].Process.Pid))
	}

	for i := range cmds {
		// Kill the process
		err = cmds[i].Process.Kill()
		assert.NoError(t, err)

		err = cmds[i].Wait()
		assert.Error(t, err) // We expect it to return KILLED
	}

	// Check the process tree is smaller
	pids3, err := GetPids(pid)
	assert.NoError(t, err)
	assert.Less(t, len(pids3), len(pids2))
}
