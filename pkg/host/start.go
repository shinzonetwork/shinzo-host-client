package host

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

// this is a new start
func Start(ctx context.Context, cfg *hostconfig.Config, log *zap.Logger, keys NodeKeys, defra DefraService) (srv *hostserver.Server, err error) {
	// start node wide server, this will spun a mux that would be used process wide.
	srv, err = hostserver.New(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("building host server: %w", err)
	}

	// start defra process, tucked defra into it's interface to reduce noice and invert depency for easy tests
	if err := defra.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting defra: %w", err)
	}

	// close defra DB when func closes as last resort even if we register a stop
	defer func() {
		if err != nil {
			_ = defra.Stop(context.Background())
		}
	}()
	srv.RegisterShutdown(func(ctx context.Context) error { return defra.Stop(ctx) })

	// fast sync from snapshots if config is enabled and provided indexer is reacable
	defra.Bootstrap(ctx)

	// mount all handlers to our servers
	if err := mountServices(srv, cfg, keys, defra); err != nil {
		return nil, err
	}

	// TODO: changing host to only join views of pools that hosts has joined or will join, not via a
	// ShinzoHub event subscription.

	// TODO: ACP/billing isn't wired up, blocked on the accounting service
	// (external, not built yet). Not a priority right now.

	// after all mounts of all handlers, start the http server
	if err := srv.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting host server: %w", err)
	}

	return srv, nil
}

func mountServices(srv *hostserver.Server, cfg *hostconfig.Config, keys NodeKeys, defra DefraService) error {
	mountHealth(srv.Mux())

	if err := mountNodeInfo(srv, keys); err != nil {
		return fmt.Errorf("mounting node info: %w", err)
	}
	if err := mountConsole(srv); err != nil {
		return fmt.Errorf("mounting console: %w", err)
	}
	if err := mountGraphQL(srv, defra.DB(), defra.Options()); err != nil {
		return fmt.Errorf("mounting graphql: %w", err)
	}
	if err := mountPlayground(srv, cfg); err != nil {
		return fmt.Errorf("mounting playground: %w", err)
	}

	return nil
}
