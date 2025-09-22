package tests

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	binaryPath := testBuild(m)
	exitCode := m.Run()
	os.Remove(binaryPath)
	os.Exit(exitCode)
}

func testBuild(m *testing.M) string {
	projectRoot := getProjectRootFromTestMain(m)
	binaryPath := getBinaryPath(projectRoot)

	// Clean up any existing binary
	os.Remove(binaryPath)

	// Build the project
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd")
	buildCmd.Dir = projectRoot
	buildOutput, err := buildCmd.CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("Failed to build project: %s", string(buildOutput)))
	}

	// Verify the binary was created
	_, err = os.Stat(binaryPath)
	if err != nil {
		panic(fmt.Sprintf("Binary was not created at %s", binaryPath))
	}
	return binaryPath
}

func getProjectRoot(t *testing.T) string {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "Failed to get current file path")
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")
	return projectRoot
}

func getProjectRootFromTestMain(t *testing.M) string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get current file path from test main")
	}
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")
	return projectRoot
}

func getBinaryPath(projectRoot string) string {
	binaryName := "testhost"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binaryPath := filepath.Join(projectRoot, "bin", binaryName)

	return binaryPath
}

func TestBuildAndRun(t *testing.T) {
	projectRoot := getProjectRoot(t)
	binaryPath := getBinaryPath(projectRoot)

	cmd := exec.Command(binaryPath)
	cmd.Dir = projectRoot

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to run the application. Output: %s", string(output))
	require.NotNil(t, output)
}
