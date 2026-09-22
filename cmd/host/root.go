package main

import "github.com/spf13/cobra"

func rootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "host",
		Short: "Run and Manage Shinzo Host",
		Long: `Shinzo host; it pulls primitive blockchain data
from Gen Client, runs Lens WASM transforms, and serves the resulting
Views.`,
		SilenceUsage: true,
	}

	cmd.AddCommand(initCmd())
	cmd.AddCommand(startCmd())
	cmd.AddCommand(versionCmd())
	cmd.AddCommand(registerCmd())

	return cmd
}

func Execute() error {
	return rootCmd().Execute()
}
