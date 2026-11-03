package host

import (
	"context"
	"net/http"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// DefraService runs the DefraDB node a host serves data from.
type DefraService interface {
	// Start brings up the DefraDB node: starting it, applying the
	// schema, and setting its P2P options.
	Start(ctx context.Context) error

	// Bootstrap imports historical snapshots from the indexer, before
	// anything else touches the node.
	Bootstrap(ctx context.Context)

	// MaintainPeerConnections dials the configured bootstrap peers and
	// starts the P2P network.
	MaintainPeerConnections(ctx context.Context)

	// AttestSignatures verifies and attests block signatures from
	// documents peers write in. In StartHosting today this shares one
	// event subscription and dispatch loop with TrackDocumentMetrics.
	AttestSignatures(ctx context.Context)

	// TrackDocumentMetrics counts documents received, per chain and
	// collection. In StartHosting today this shares one event
	// subscription and dispatch loop with AttestSignatures.
	TrackDocumentMetrics(ctx context.Context)

	// PruneDocuments runs the per-chain pruners that drop old
	// replicated blocks.
	PruneDocuments(ctx context.Context)

	// Stop closes the P2P network, stops the pruners, then closes the
	// DefraDB node last, since the API server has to stop reading from
	// it first.
	Stop(ctx context.Context) error

	// Views returns the view manager, or nil before Start has built one.
	Views() *view.Manager

	// Handler returns the API handler, or nil before Start has built one.
	Handler() http.Handler
}
