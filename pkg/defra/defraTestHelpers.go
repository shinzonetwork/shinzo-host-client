package defra

import (
	"context"
	"testing"

	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
)

func StartDefraInstance(t *testing.T, ctx context.Context, options []node.Option) *node.Node {
	myNode, err := node.New(ctx, options...)
	assert.NoError(t, err)
	assert.NotNil(t, myNode)

	err = myNode.Start(ctx)
	assert.NoError(t, err)

	return myNode
}
