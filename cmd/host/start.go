package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/keys"
)

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Run the Shinzo Host",
		Long: `Run the Shinzo Host.

Requires an already-initialized instance: run init first. start never
creates config.toml and never generates keys on its own. Use
--recover-file to recover keys specifically, if config.toml already
exists but keys don't.`,
		Example: `  host start
  host start --config /data/shinzo-host/config.toml
  host start --home /data/shinzo-host
  host start --data-dir mnt/folder
  host start --home /data/shinzo-host --data-dir mnt/folder
  host start --logger.level warn --http.addr :9090
  host start --recover-file /path/to/mnemonic.txt`,
		RunE: runStart,
	}

	registerCommonFlags(cmd)
	cmd.Flags().String("config", "", "explicit path to config.toml (default <home>/config.toml)")
	cmd.Flags().String("recover-file", "", "recover an existing node from a file containing its mnemonic seed phrase")

	return cmd
}

func runStart(cmd *cobra.Command, _ []string) (err error) {
	recoverMnemonic, err := recoverMnemonicFromFlag(cmd)
	if err != nil {
		return err
	}

	home, err := cmd.Flags().GetString("home")
	if err != nil {
		return err
	}
	dataDir, err := cmd.Flags().GetString("data-dir")
	if err != nil {
		return err
	}
	configPath, err := cmd.Flags().GetString("config")
	if err != nil {
		return err
	}
	overrides, err := getOverrides(cmd)
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

	log, flush, err := newHostLogger(cfg)
	if err != nil {
		return err
	}
	defer flush(&err)

	nodeKeys, err := resolveStartKeys(cfg, recoverMnemonic, log)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Loaded config for Shinzo Host at %s\n", cfg.Home); err != nil {
		return err
	}

	return printConfig(cmd.OutOrStdout(), cfg, nodeKeys)
}

// resolveStartKeys loads the node's keys, it never generates one.
func resolveStartKeys(cfg hostconfig.Config, recoverMnemonic string, log *zap.Logger) (keys.NodeKeys, error) {
	nodeKeys, _, err := keys.EnsureKeys(cfg.KeyDir, recoverMnemonic, false, log.Sugar())
	if err != nil {
		switch {
		case errors.Is(err, keys.ErrNoKeys):
			return keys.NodeKeys{}, fmt.Errorf("no keys found at %s: run \"host init\" first, or pass --%s: %w", cfg.KeyDir, recoverFileFlag, err)
		case recoverMnemonic != "" && errors.Is(err, keys.ErrRecoverMismatch):
			return keys.NodeKeys{}, fmt.Errorf("--%s: %w", recoverFileFlag, err)
		default:
			return keys.NodeKeys{}, err
		}
	}

	return nodeKeys, nil
}
