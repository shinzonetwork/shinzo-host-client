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

// new entry point, built from scratch next to StartHosting, not replacing
// it. Uses hostconfig.Config, not config.Config, and returns
// *hostserver.Server, not *Host, on purpose, this doesn't touch the old
// struct at all.
func Start(ctx context.Context, cfg *hostconfig.Config, log *zap.Logger) (*hostserver.Server, error) {
	if _, err := buildACPConfig(cfg); err != nil {
		return nil, fmt.Errorf("acp config: %w", err)
	}

	srv, err := hostserver.New(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("building host server: %w", err)
	}

	defraNode, err := startDefra(ctx, srv, cfg, log)
	if err != nil {
		return nil, fmt.Errorf("starting defra: %w", err)
	}

	mountHealth(srv.Mux())
	// TODO: mountGraphQL(srv, defraNode, cfg, log), mountPlayground(srv, cfg),
	// startEventSubscription(ctx, defraNode, ...), each mounts itself, same
	// as startDefra and mountHealth do.
	_ = defraNode

	if err := srv.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting host server: %w", err)
	}

	return srv, nil
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
