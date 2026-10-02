package main

import "github.com/spf13/cobra"

func registerCommonFlags(cmd *cobra.Command) {
	cmd.Flags().String("home", "", "instance home directory (default ~/.shinzo/host)")
	cmd.Flags().String("data-dir", "", "data directory (default <home>/data)")

	cmd.Flags().Bool("logger.development", false, "override logger.development")
	cmd.Flags().String("logger.level", "", "override logger.level (debug, info, warn, error)")
	cmd.Flags().String("http.addr", "", "override http.addr, e.g. :8080")
}
