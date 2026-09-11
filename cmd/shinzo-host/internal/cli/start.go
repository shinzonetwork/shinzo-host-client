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

const shutdownTimeout = 30 * time.Second

var recoverMnemonic string

func newStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the host node",
		RunE:  runStart,
	}
	cmd.Flags().StringVar(&recoverMnemonic, "recover", "",
		"BIP39 mnemonic to recover this node's keys from. Ignored if keys already "+
			"exist, only used the first time this instance starts. Leave empty to "+
			"generate a new one.")
	return cmd
}

func runStart(cmd *cobra.Command, _ []string) error {
	cfg, err := hostconfig.Load(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log, syncLog, err := logger.New(logger.Config{
		Level:       cfg.Logger.Level,
		Development: cfg.Logger.Development,
		Fields:      map[string]any{"service": "shinzo-host"},
	})
	if err != nil {
		return fmt.Errorf("building logger: %w", err)
	}
	defer func() { _ = syncLog() }()

	logger.Sugar = log.Sugar()

	keys, err := host.EnsureKeys(cfg, recoverMnemonic, log.Sugar())
	if err != nil {
		return fmt.Errorf("ensuring keys: %w", err)
	}

	defra := host.NewDefraService(cfg, log, keys.IdentityKey, keys.PeerKeySeed)

	h, err := host.Start(ctx, cfg, log, keys, defra)
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
