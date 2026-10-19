package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/keys"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/logger"
)

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Run the Shinzo Host",
		Long: `Run the Shinzo Host.

If no config exists yet at the resolved location, start creates one from
defaults automatically. Running init first is optional, not required.
Use init instead when you want to set things up, e.g. pass overrides to
save, before the first start.`,
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

	cfg, err := loadStartConfig(cmd)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Loaded config for Shinzo Host at %s\n", cfg.Home); err != nil {
		return err
	}

	log, sync, err := logger.New(logger.Config{
		Level:       cfg.Logger.Level,
		Development: cfg.Logger.Development,
	})
	if err != nil {
		return err
	}
	defer func() {
		// Only surfaced if nothing else already failed: a flush error
		// shouldn't mask whatever real error the command hit first.
		if syncErr := sync(); syncErr != nil && err == nil {
			err = syncErr
		}
	}()

	nodeKeys, err := resolveStartKeys(cmd, cfg, recoverMnemonic, log)
	if err != nil {
		return err
	}

	return printConfig(cmd.OutOrStdout(), cfg, nodeKeys)
}

func loadStartConfig(cmd *cobra.Command) (hostconfig.Config, error) {
	home, err := cmd.Flags().GetString("home")
	if err != nil {
		return hostconfig.Config{}, err
	}

	dataDir, err := cmd.Flags().GetString("data-dir")
	if err != nil {
		return hostconfig.Config{}, err
	}

	configPath, err := cmd.Flags().GetString("config")
	if err != nil {
		return hostconfig.Config{}, err
	}

	overrides, err := getOverrides(cmd)
	if err != nil {
		return hostconfig.Config{}, err
	}

	return hostconfig.Load(home, dataDir, configPath, overrides)
}

// resolveStartKeys loads the node's keys, never generating one on its
// own (start requires init or --recover-file for that), and prints any
// freshly generated mnemonic EnsureKeys reports.
func resolveStartKeys(cmd *cobra.Command, cfg hostconfig.Config, recoverMnemonic string, log *zap.Logger) (keys.NodeKeys, error) {
	nodeKeys, mnemonic, err := keys.EnsureKeys(cfg.KeyDir, recoverMnemonic, false, log.Sugar())
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

	if err := printGeneratedMnemonic(cmd.OutOrStdout(), mnemonic); err != nil {
		return keys.NodeKeys{}, err
	}

	return nodeKeys, nil
}
