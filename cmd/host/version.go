package main

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:     "version",
	Short:   "Print Shinzo Host version",
	Example: `  host version`,
	RunE:    runVersion,
}

func runVersion(cmd *cobra.Command, args []string) error {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		fmt.Println("unknown")
		return nil
	}

	fmt.Fprintln(cmd.OutOrStdout(), info.Main.Path, info.GoVersion)
	return nil
}
