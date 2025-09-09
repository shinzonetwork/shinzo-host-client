package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildAndRun(t *testing.T) {
	// Get the project root directory
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "Failed to get current file path")
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")

	// Build the binary
	binaryName := "host"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binaryPath := filepath.Join(projectRoot, "bin", binaryName)

	// Clean up any existing binary
	os.Remove(binaryPath)

	// Build the project
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd")
	buildCmd.Dir = projectRoot
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build project: %s", string(buildOutput))

	// Verify the binary was created
	_, err = os.Stat(binaryPath)
	require.NoError(t, err, "Binary was not created at %s", binaryPath)

	// Create a temporary directory for the defra store
	tempDir := t.TempDir()
	defraStorePath := filepath.Join(tempDir, ".defra")

	// Run the binary with defra store path and wait for it to complete
	cmd := exec.Command(binaryPath, "-defra-store-path", defraStorePath)
	cmd.Dir = projectRoot

	// Run the command and capture output
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to run the application. Output: %s", string(output))
	require.NotNil(t, output)

	// The process should complete successfully and exit naturally
	t.Logf("Application completed successfully. Output: %s", string(output))

	// Clean up the binary
	os.Remove(binaryPath)

	t.Logf("Successfully built and ran the application with defra store at: %s", defraStorePath)
}
