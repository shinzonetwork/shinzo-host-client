package defra

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func StartDefraInstance(t *testing.T, ctx context.Context, options []node.Option) *node.Node {
	myNode, err := node.New(ctx, options...)
	require.NoError(t, err)
	require.NotNil(t, myNode)

	err = myNode.Start(ctx)
	require.NoError(t, err)

	return myNode
}

func GetPort(targetNode *node.Node) int {
	url := targetNode.APIURL

	// Check if it's localhost format (http://127.0.0.1:port or http://localhost:port)
	if !strings.Contains(url, "127.0.0.1:") && !strings.Contains(url, "localhost:") {
		return -1
	}

	// Extract port by splitting on colon and taking the last part
	parts := strings.Split(url, ":")
	if len(parts) < 2 {
		return -1
	}

	// Get the last part (port) and remove any trailing path
	portStr := parts[len(parts)-1]
	if strings.Contains(portStr, "/") {
		portStr = strings.Split(portStr, "/")[0]
	}

	// Convert port string to int
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		return -1
	}

	return portInt
}
