// Package host is the orchestration layer for running a Shinzo Host
// instance: binding its HTTP server and mounting everything it
// serves onto it.
package host

import (
	"time"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostserver"
)

// Start the host.
func Start(cfg hostconfig.Config, log *zap.Logger, keys hostkey.NodeKeys, srv *hostserver.Server) error {
	// cfg, log and keys aren't used by any mount yet, kept as params so
	// this signature won't need to change when one needs them.
	_ = cfg
	_ = log
	_ = keys

	mountStatus(srv.Mux(), time.Now())

	return srv.Start()
}
