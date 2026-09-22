package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// version is set at build time via -ldflags, see the build-staging Makefile target.
var version = "dev"

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "version",
		Short:   "Print Shinzo Host version",
		Example: `  host version`,
		RunE:    runVersion,
	}
}

func runVersion(cmd *cobra.Command, _ []string) error {
	_, err := fmt.Fprintln(cmd.OutOrStdout(), version)
	return err
}
