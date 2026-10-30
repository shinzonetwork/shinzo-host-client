// Package host is the orchestration layer for running a Shinzo Host
// instance: binding its HTTP server and mounting everything it
// serves onto it.
package host

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostserver"
)

// Start the host: mounts everything it serves onto srv, binds it, and
// blocks until ctx is done (a signal, or srv failing on its own).
//
// cfg and keys aren't used by any mount yet, kept as params so this
// signature won't need to change when one needs them.
func Start(ctx context.Context, _ hostconfig.Config, log *zap.Logger, _ hostkey.NodeKeys, srv *hostserver.Server) error {
	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mountStatus(srv.Mux(), time.Now())
	srv.OnFailure(stop)

	if err := srv.Start(); err != nil {
		_ = srv.Close() //nolint:contextcheck // Close takes no context, nothing to pass through
		return err
	}
	log.Sugar().Infow("listening", "addr", srv.Addr())

	<-runCtx.Done()
	stop()             // let a second signal force kill instead of waiting out the shutdown timeout
	return srv.Close() //nolint:contextcheck // Close takes no context, nothing to pass through
}
