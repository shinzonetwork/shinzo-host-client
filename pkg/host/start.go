package host

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/acp"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func Start(ctx context.Context, cfg *hostconfig.Config, log *zap.Logger, keys NodeKeys, defra DefraService) (srv *hostserver.Server, err error) {
	if _, err := buildACPConfig(cfg); err != nil {
		return nil, fmt.Errorf("acp config: %w", err)
	}

	srv, err = hostserver.New(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("building host server: %w", err)
	}

	if err := defra.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting defra: %w", err)
	}
	// Anything that fails below this point leaves defra running with
	// nothing left holding a reference to stop it, clean it up ourselves.
	defer func() {
		if err != nil {
			_ = defra.Stop(context.Background())
		}
	}()
	srv.RegisterShutdown(func(ctx context.Context) error { return defra.Stop(ctx) })

	if err := mountServices(srv, cfg, keys, defra); err != nil {
		return nil, err
	}

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

func buildACPConfig(cfg *hostconfig.Config) (acp.Config, error) {
	var window time.Duration
	if cfg.ACP.AttesterWindow != "" {
		var err error
		window, err = time.ParseDuration(cfg.ACP.AttesterWindow)
		if err != nil {
			return acp.Config{}, fmt.Errorf("acp.attester_window: %w", err)
		}
	}

	acpCfg := acp.Config{
		Enabled:         cfg.ACP.Enabled,
		ChainID:         cfg.Shinzo.ChainID,
		MinQueryBalance: cfg.ACP.MinQueryBalance,
		EpochLength:     cfg.ACP.EpochLength,
		ASBaseURL:       cfg.ACP.ASBaseURL,
		AttesterWindow:  window,
	}

	if err := acpCfg.Validate(); err != nil {
		return acp.Config{}, err
	}

	return acpCfg, nil
}
