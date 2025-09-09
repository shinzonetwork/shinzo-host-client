package host

import (
	"context"
	"testing"

	"github.com/shinzonetwork/host/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
)

func TestStartHosting(t *testing.T) {
	err := StartHosting("../.defra", "http://localhost:9181")

	assert.NoError(t, err)
}

func TestStartHostingWithOwnDefraInstance(t *testing.T) {
	ctx := context.Background()

	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(true),
		node.WithStorePath(t.TempDir()),
	}

	myNode := defra.StartDefraInstance(t, ctx, options)
	assert.NotNil(t, myNode)
	defer myNode.Close(ctx)

	err := StartHosting("", "http://localhost:9181")

	assert.NoError(t, err)
}
