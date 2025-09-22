package tests

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuickStart(t *testing.T) {
	cmd := exec.Command("go", "run", "cmd/main.go")
	projectRoot := getProjectRoot(t)
	cmd.Dir = projectRoot

	output, err := cmd.CombinedOutput()

	assert.NoError(t, err, "Failed to run the application. Output: %s", string(output))
	assert.NotNil(t, output)
}
