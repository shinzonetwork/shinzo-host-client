package host

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
)

// TODO: replace once NewDefra is implemented. The tests in this file pin
// the stub's behaviour, not the DefraService contract, so delete them
// rather than update them when the real implementation lands.

// Not implemented yet, so every method must be a harmless no-op.
func TestDefraServiceMethodsAreNoOps(t *testing.T) {
	defra := NewDefra(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{})
	ctx := context.Background()

	require.NoError(t, defra.Start(ctx))
	require.NoError(t, defra.Run(ctx))
	require.NoError(t, defra.Stop(ctx))
}

// Views has nothing to return before Start has built a node to build one from.
func TestDefraServiceViewsIsNilBeforeStart(t *testing.T) {
	defra := NewDefra(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{})
	require.Nil(t, defra.Views())
}

// Handler has nothing to return before Start has built one.
func TestDefraServiceHandlerIsNilBeforeStart(t *testing.T) {
	defra := NewDefra(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{})
	require.Nil(t, defra.Handler())
}
