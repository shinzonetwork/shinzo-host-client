package main

import (
	"errors"

	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:     "start",
	Short:   "Run the Shinzo Host ",
	Example: `  host start`,
	RunE:    runStart,
}

func runStart(cmd *cobra.Command, args []string) error {
	return errors.New("start not implemented")
}
