package main

import (
	"github.com/spf13/cobra"
)

func initCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize a new Shinzo Host instance",
		Example: `  host init
  host init --name examplenode`,
		RunE: runInit,
	}
}

func runInit(_ *cobra.Command, _ []string) error {
	return errNotImplemented
}
