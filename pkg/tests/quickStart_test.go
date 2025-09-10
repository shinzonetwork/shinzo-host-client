package tests

import (
	"context"
	"os/exec"
	"testing"

	"github.com/shinzonetwork/host/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
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
