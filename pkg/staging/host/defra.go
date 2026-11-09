package host

import (
	"context"
	"net/http"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// DefraService runs the DefraDB node a host serves data from.
//
// Call order is Start, then Run, then Stop. The steps that must happen
// in a fixed sequence live inside Start and Run, so callers can't get
// them out of order.
type DefraService interface {
	// Start brings the node up and gets it ready to serve, in order:
	// start the node (schema, P2P options), subscribe to its update
	// events (signature attestation and document metrics share that one
	// subscription), import historical snapshots, then build the view
	// manager and API handler. The subscription comes before anything
	// that can write to the node so no update goes unattested.
	Start(ctx context.Context) error

	// Run connects to the network and starts background upkeep: dials
	// the configured bootstrap peers and starts the per-chain pruners.
	// Must be called after Start, so peer updates are already being
	// attested when they arrive.
	Run(ctx context.Context) error

	// Stop closes the P2P network, stops the pruners, then closes the
	// DefraDB node last, since the API server has to stop reading from
	// it first.
	Stop(ctx context.Context) error

	// Views returns the view manager, or nil before Start has built one.
	Views() *view.Manager

	// Handler returns the API handler, or nil before Start has built one.
	Handler() http.Handler
}
