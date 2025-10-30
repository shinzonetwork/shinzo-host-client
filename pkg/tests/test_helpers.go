package tests

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// startDefraInstanceForTest creates a DefraDB node for testing purposes
func startDefraInstanceForTest(t *testing.T, ctx context.Context, options []node.Option) *node.Node {
	myNode, err := node.New(ctx, options...)
	require.NoError(t, err)
	require.NotNil(t, myNode)

	err = myNode.Start(ctx)
	require.NoError(t, err)

	return myNode
}

// getProjectRoot returns the project root directory
func getProjectRoot(t *testing.T) string {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "Failed to get current file path")
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")
	return projectRoot
}
