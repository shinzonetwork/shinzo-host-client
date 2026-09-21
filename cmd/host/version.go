package main

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "version",
		Short:   "Print Shinzo Host version",
		Example: `  host version`,
		RunE:    runVersion,
	}
}

func runVersion(cmd *cobra.Command, _ []string) error {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		_, err := fmt.Fprintln(cmd.OutOrStdout(), "unknown")
		return err
	}

	_, err := fmt.Fprintln(cmd.OutOrStdout(), info.Main.Path, info.GoVersion)
	return err
}
