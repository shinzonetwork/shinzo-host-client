package cli

import (
	"github.com/spf13/cobra"
)

var configPath string

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "shinzo-host",
		Short:         "Run and manage a Shinzo host node",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVar(&configPath, "config", "", "path to config.toml (default: XDG_DATA_HOME/shinzo-host/default/config.toml)")

	root.AddCommand(newStartCmd())

	return root
}

func Execute() error {
	return newRootCmd().Execute()
}
