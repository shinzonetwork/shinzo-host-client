package main

import (
	"errors"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a new Shinzo Host instance",
	Example: `  host init
  host init --name examplenode`,
	RunE: runInit,
}

func runInit(cmd *cobra.Command, args []string) error {
	return errors.New("init not implemented")
}
