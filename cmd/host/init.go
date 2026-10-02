package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
)

func initCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a new Shinzo Host instance",
		Example: `  host init
  host init --home /data/shinzo-host
  host init --data-dir mnt/folder
  host init --home /data/shinzo-host --data-dir mnt/folder
  host init --logger.level warn --http.addr :9090`,
		RunE: runInit,
	}

	registerCommonFlags(cmd)

	return cmd
}

func runInit(cmd *cobra.Command, _ []string) error {
	home, err := cmd.Flags().GetString("home")
	if err != nil {
		return err
	}

	dataDir, err := cmd.Flags().GetString("data-dir")
	if err != nil {
		return err
	}

	overrides, err := getOverrides(cmd)
	if err != nil {
		return err
	}

	cfg, err := hostconfig.Setup(home, dataDir, overrides)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Initialized Shinzo Host at %s\n", cfg.Home); err != nil {
		return err
	}

	return printConfig(cmd.OutOrStdout(), cfg)
}
