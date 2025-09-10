package tests

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/shinzonetwork/host/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
)

func testBuild(t *testing.T) (string, string) {
	projectRoot := getProjectRoot(t)

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
	assert.NoError(t, err, "Failed to build project: %s", string(buildOutput))

	// Verify the binary was created
	_, err = os.Stat(binaryPath)
	assert.NoError(t, err, "Binary was not created at %s", binaryPath)

	return projectRoot, binaryPath
}

func getProjectRoot(t *testing.T) string {
	_, filename, _, ok := runtime.Caller(0)
	assert.True(t, ok, "Failed to get current file path")
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")
	return projectRoot
}

func TestBuildAndRun(t *testing.T) {
	projectRoot, binaryPath := testBuild(t)

	cmd := exec.Command(binaryPath)
	cmd.Dir = projectRoot

	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, "Failed to run the application. Output: %s", string(output))
	assert.NotNil(t, output)

	os.Remove(binaryPath)
}

func TestBuildAndRun_NoDefra(t *testing.T) {
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(true),
		node.WithStorePath(t.TempDir()),
	}
	ctx := context.Background()
	myNode := defra.StartDefraInstance(t, ctx, options)
	assert.NotNil(t, myNode)
	defer myNode.Close(ctx)

	projectRoot, binaryPath := testBuild(t)

	cmd := exec.Command(binaryPath, "-defra-started=true")
	cmd.Dir = projectRoot

	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, "Failed to run the application. Output: %s", string(output))
	assert.NotNil(t, output)

	os.Remove(binaryPath)
}

func TestBuildAndRun_DefraInBackgroundFail(t *testing.T) {
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(true),
		node.WithStorePath(t.TempDir()),
	}
	ctx := context.Background()
	myNode := defra.StartDefraInstance(t, ctx, options)
	assert.NotNil(t, myNode)
	defer myNode.Close(ctx)

	projectRoot, binaryPath := testBuild(t)

	cmd := exec.Command(binaryPath, "-defra-started=false")
	cmd.Dir = projectRoot

	output, err := cmd.CombinedOutput()
	assert.Error(t, err, "Expected an error, but got none")
	assert.NotNil(t, output)

	os.Remove(binaryPath)
}

func TestQuickStart(t *testing.T) {
	cmd := exec.Command("go", "run", "cmd/main.go")
	projectRoot := getProjectRoot(t)
	cmd.Dir = projectRoot

	output, err := cmd.CombinedOutput()

	assert.NoError(t, err, "Failed to run the application. Output: %s", string(output))
	assert.NotNil(t, output)
}

func TestQuickStart_NoDefra(t *testing.T) {
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(true),
		node.WithStorePath(t.TempDir()),
	}
	ctx := context.Background()
	myNode := defra.StartDefraInstance(t, ctx, options)
	assert.NotNil(t, myNode)
	defer myNode.Close(ctx)

	cmd := exec.Command("go", "run", "cmd/main.go", "-defra-started=true")
	projectRoot := getProjectRoot(t)
	cmd.Dir = projectRoot
	output, err := cmd.CombinedOutput()

	assert.NoError(t, err, "Failed to run the application. Output: %s", string(output))
	assert.NotNil(t, output)
}

func TestQuickStart_DefraInBackgroundFail(t *testing.T) {
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(true),
		node.WithStorePath(t.TempDir()),
	}
	ctx := context.Background()
	myNode := defra.StartDefraInstance(t, ctx, options)
	assert.NotNil(t, myNode)
	defer myNode.Close(ctx)

	cmd := exec.Command("go", "run", "cmd/main.go", "-defra-started=false")
	projectRoot := getProjectRoot(t)
	cmd.Dir = projectRoot
	output, err := cmd.CombinedOutput()

	assert.Error(t, err, "Expected an error, but got none")
	assert.NotNil(t, output)
}
