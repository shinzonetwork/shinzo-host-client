package host

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func Start(ctx context.Context, cfg *hostconfig.Config, log *zap.Logger, keys NodeKeys, defra DefraService) (srv *hostserver.Server, err error) {
	srv, err = hostserver.New(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("building host server: %w", err)
	}

	if err := defra.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting defra: %w", err)
	}

	defer func() {
		if err != nil {
			_ = defra.Stop(context.Background())
		}
	}()
	srv.RegisterShutdown(func(ctx context.Context) error { return defra.Stop(ctx) })

	if err := srv.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting host server: %w", err)
	}

	return srv, nil
}
