package defra

import (
	"context"
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
