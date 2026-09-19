package main

import (
	"github.com/spf13/cobra"
)

func startCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "start",
		Short:   "Run the Shinzo Host ",
		Example: `  host start`,
		RunE:    runStart,
	}
}

func runStart(_ *cobra.Command, _ []string) error {
	return errNotImplemented
}
