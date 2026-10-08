package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
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
  host start --logger.level warn --http.addr :9090`,
		RunE: runStart,
	}

	registerCommonFlags(cmd)
	cmd.Flags().String("config", "", "explicit path to config.toml (default <home>/config.toml)")

	return cmd
}

func runStart(cmd *cobra.Command, _ []string) error {
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
		return err
	}

	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Loaded config for Shinzo Host at %s\n", cfg.Home); err != nil {
		return err
	}

	return printConfig(cmd.OutOrStdout(), cfg)
}
