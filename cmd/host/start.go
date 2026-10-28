package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/host"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostserver"
)

const shutdownTimeout = 30 * time.Second

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Run the Shinzo Host",
		Long: `Run the Shinzo Host.

Requires an already-initialized instance: run init first. start never
creates config.toml and never generates keys on its own.`,
		Example: `  host start
  host start --config /data/shinzo-host/config.toml
  host start --home /data/shinzo-host
  host start --data-dir mnt/folder
  host start --home /data/shinzo-host --data-dir mnt/folder
  host start --logger.level warn --http.addr :9090`,
		RunE: runStart,
	}

	registerCommonFlags(cmd)
	cmd.Flags().String("config", "", "explicit path to config.toml (default <home>/config.toml)")

	return cmd
}

func runStart(cmd *cobra.Command, _ []string) (err error) {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	home, dataDir, configPath, overrides, err := parseStartFlags(cmd)
	if err != nil {
		return err
	}

	cfg, err := hostconfig.Load(home, dataDir, configPath, overrides)
	if err != nil {
		if errors.Is(err, hostconfig.ErrConfigNotFound) {
			return fmt.Errorf("%w: run \"host init\" first", err)
		}
		return err
	}

	nodeKeys, err := hostkey.Load(cfg.KeyDir)
	if err != nil {
		if errors.Is(err, hostkey.ErrNoKeys) {
			return fmt.Errorf("%w at %s: run \"host init\" to set up new keys, or \"host init --%s <file>\" to restore existing ones",
				err, cfg.KeyDir, recoverFileFlag)
		}
		return err
	}

	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Loaded config for Shinzo Host at %s\n", cfg.Home); err != nil {
		return err
	}

	if err := printConfig(cmd.OutOrStdout(), cfg, nodeKeys); err != nil {
		return err
	}

	log, flush, err := newHostLogger(cfg)
	if err != nil {
		return err
	}
	defer flush(&err)

	srv, err := hostserver.New(cfg.HTTP.Addr, log)
	if err != nil {
		return err
	}

	defra := host.NewDefraService(cfg, log, nodeKeys)

	if err := host.Start(cfg, log, nodeKeys, defra, srv); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Listening on %s\n", cfg.HTTP.Addr); err != nil {
		return err
	}

	<-ctx.Done()
	stop() // let a second signal force kill instead of waiting out the shutdown timeout

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Close(shutdownCtx)
}

func parseStartFlags(cmd *cobra.Command) (home, dataDir, configPath string, overrides map[string]any, err error) {
	home, err = cmd.Flags().GetString("home")
	if err != nil {
		return "", "", "", nil, err
	}
	dataDir, err = cmd.Flags().GetString("data-dir")
	if err != nil {
		return "", "", "", nil, err
	}
	configPath, err = cmd.Flags().GetString("config")
	if err != nil {
		return "", "", "", nil, err
	}
	overrides, err = getOverrides(cmd)
	if err != nil {
		return "", "", "", nil, err
	}
	return home, dataDir, configPath, overrides, nil
}
