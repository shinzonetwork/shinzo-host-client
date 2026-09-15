package host

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

func Start(ctx context.Context, cfg *config.Config, log *zap.Logger, keys NodeKeys, defra DefraService) (srv *hostserver.Server, err error) {
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

	defra.Bootstrap(ctx)

	defra.MaintainPeerConnections(ctx)

	defra.AttestSignatures(ctx)

	defra.TrackDocumentMetrics(ctx)

	defra.PruneDocuments(ctx)

	// TODO: changing host to only join views of pools that hosts has joined or will join, not via a
	// ShinzoHub event subscription.

	// TODO: ACP/billing isn't wired up, blocked on the accounting service
	// (external, not built yet). Not a priority right now.

	// TODO: the old batched attestation pipeline (ProcessingPipeline, gated by
	// use_block_signatures) was never ported over. It was already inactive in production
	// and benchmarked slower than the current per-document writes under contention, so
	// it's not missed, but the config field still sits there implying a working switch.
	// Remove the field or wire it up for real.

	if err := mountServices(srv, cfg, keys, defra, log); err != nil {
		return nil, err
	}

	if err := srv.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting host server: %w", err)
	}

	shinzoAddress, err := keys.ShinzoAddress()
	if err != nil {
		return nil, fmt.Errorf("resolving shinzo address: %w", err)
	}

	sugar := log.Sugar()
	if logger.IsTerminal() {
		sugar.Info("\n" + renderBox("Identity", [][2]string{
			{"shinzo", shinzoAddress},
			{"evm", keys.OperatorAddress().Hex()},
			{"did", keys.DID()},
			{"p2p", cfg.P2P.ListenAddr},
		}))
	} else {
		sugar.Infow("host started",
			"shinzo_address", shinzoAddress,
			"evm_address", keys.OperatorAddress().Hex(),
			"did", keys.DID(),
			"p2p", cfg.P2P.ListenAddr,
		)
	}

	return srv, nil
}

func mountServices(srv *hostserver.Server, cfg *config.Config, keys NodeKeys, defra DefraService, log *zap.Logger) error {
	mountHealth(srv, defra)
	mountMetrics(srv, defra)
	mountSystemStats(srv, cfg)

	if err := mountNodeInfo(srv, keys); err != nil {
		return fmt.Errorf("mounting node info: %w", err)
	}
	if err := mountConsole(srv); err != nil {
		return fmt.Errorf("mounting console: %w", err)
	}
	if err := mountGraphQL(srv, defra.DB(), defra.Options()); err != nil {
		return fmt.Errorf("mounting graphql: %w", err)
	}
	playgroundMounted := cfg.Playground.Enabled
	if err := mountPlayground(srv, cfg); err != nil {
		return fmt.Errorf("mounting playground: %w", err)
	}

	base := displayBaseURL(cfg.HTTP.Addr)
	routes := [][2]string{
		{"health", base + "/health"},
		{"metrics", base + "/metrics"},
		{"system stats", base + "/api/system"},
		{"node info", base + "/api/node"},
		{"console", base + "/console"},
		{"graphql", base + "/api/"},
	}
	if playgroundMounted {
		routes = append(routes, [2]string{"playground", base + "/playground"})
	}

	sugar := log.Sugar()
	if logger.IsTerminal() {
		sugar.Info("\n" + renderBox("Routes", routes))
	} else {
		for _, r := range routes {
			sugar.Infow(r[0], "url", r[1])
		}
	}

	return nil
}

func displayBaseURL(addr string) string {
	host := addr
	switch {
	case strings.HasPrefix(host, ":"):
		host = "localhost" + host
	case strings.HasPrefix(host, "0.0.0.0:"):
		host = "localhost" + strings.TrimPrefix(host, "0.0.0.0")
	}
	return "http://" + host
}
