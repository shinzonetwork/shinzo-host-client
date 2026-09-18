package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:   "host",
	Short: "Run and Manage Shinzo Host",
	Long: `Shinzo host; it pulls primitive blockchain data
from Gen Client, runs Lens WASM transforms, and serves the resulting
Views.`,
	SilenceUsage: true,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// add persistence flags here

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(registerCmd)
	// add commands here
}
