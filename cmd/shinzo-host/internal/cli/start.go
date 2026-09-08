package cli

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

// same value cmd/main.go uses
const shutdownTimeout = 30 * time.Second

func newStartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the host node",
		RunE:  runStart,
	}
}

func runStart(cmd *cobra.Command, _ []string) error {
	cfg, err := hostconfig.Load(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log, syncLog, err := logger.New(logger.Config{
		Development: cfg.Logger.Development,
		Fields:      map[string]any{"service": "shinzo-host"},
	})
	if err != nil {
		return fmt.Errorf("building logger: %w", err)
	}
	defer func() { _ = syncLog() }()

	h, err := host.Start(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("starting host: %w", err)
	}

	<-ctx.Done()
	log.Sugar().Info("received shutdown signal")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := h.Close(shutdownCtx); err != nil {
		return fmt.Errorf("shutting down: %w", err)
	}

	log.Sugar().Info("shutdown complete")
	return nil
}
