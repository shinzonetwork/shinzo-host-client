package host

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
)

// Not implemented yet, so every method must be a harmless no-op.
func TestDefraServiceMethodsAreNoOps(t *testing.T) {
	defra := NewDefraService(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{})
	ctx := context.Background()

	require.NoError(t, defra.Start(ctx))
	defra.Bootstrap(ctx)
	defra.MaintainPeerConnections(ctx)
	defra.AttestSignatures(ctx)
	defra.TrackDocumentMetrics(ctx)
	defra.PruneDocuments(ctx)
	require.NoError(t, defra.Stop(ctx))
}

// Views has nothing to return before Start has built a node to build one from.
func TestDefraServiceViewsIsNilBeforeStart(t *testing.T) {
	defra := NewDefraService(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{})
	require.Nil(t, defra.Views())
}

// Handler has nothing to return before Start has built one.
func TestDefraServiceHandlerIsNilBeforeStart(t *testing.T) {
	defra := NewDefraService(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{})
	require.Nil(t, defra.Handler())
}
