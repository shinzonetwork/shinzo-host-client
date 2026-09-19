package main

import (
	"github.com/spf13/cobra"
)

func registerCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "register",
		Short:   "Register this Host on ShinzoHub",
		Example: `  host register`,
		RunE:    runRegister,
	}
}

func runRegister(_ *cobra.Command, _ []string) error {
	return errNotImplemented
}
