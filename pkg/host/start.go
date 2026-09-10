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

func Start(ctx context.Context, cfg *hostconfig.Config, log *zap.Logger, keys NodeKeys) (*hostserver.Server, error) {
	if _, err := buildACPConfig(cfg); err != nil {
		return nil, fmt.Errorf("acp config: %w", err)
	}

	srv, err := hostserver.New(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("building host server: %w", err)
	}

	defraNode, err := startDefra(ctx, srv, cfg, log, keys.IdentityKey, keys.PeerKeySeed)
	if err != nil {
		return nil, fmt.Errorf("starting defra: %w", err)
	}
	srv.RegisterShutdown(func(ctx context.Context) error { return defraNode.Close(ctx) })

	mountHealth(srv.Mux())

	if err := mountNodeInfo(srv, keys); err != nil {
		return nil, fmt.Errorf("mounting node info: %w", err)
	}
	if err := mountConsole(srv); err != nil {
		return nil, fmt.Errorf("mounting console: %w", err)
	}

	if err := mountGraphQL(srv, defraNode); err != nil {
		return nil, fmt.Errorf("mounting graphql: %w", err)
	}
	if err := mountPlayground(srv, cfg); err != nil {
		return nil, fmt.Errorf("mounting playground: %w", err)
	}
	// TODO: startEventSubscription(ctx, defraNode, ...), still not wired.

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
