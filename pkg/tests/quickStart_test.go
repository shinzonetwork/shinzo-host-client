package tests

import (
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQuickStart(t *testing.T) {
	cmd := exec.Command("go", "run", "cmd/main.go")
	projectRoot := getProjectRoot(t)
	cmd.Dir = projectRoot

	// Start the host process
	err := cmd.Start()
	assert.NoError(t, err, "Failed to start the application")

	// Let it run for a few seconds to check for startup errors
	time.Sleep(3 * time.Second)

	// Check if the process is still running (it should be)
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		// If it exited, there was likely an error
		output, _ := cmd.CombinedOutput()
		assert.Fail(t, "Application exited unexpectedly. Output: %s", string(output))
	}

	// Stop the process gracefully
	err = cmd.Process.Kill()
	assert.NoError(t, err, "Failed to stop the application")

	// Wait for the process to actually terminate
	cmd.Wait()
}
