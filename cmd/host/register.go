package main

import (
	"errors"

	"github.com/spf13/cobra"
)

var registerCmd = &cobra.Command{
	Use:     "register",
	Short:   "Register this Host on ShinzoHub",
	Example: `  host register`,
	RunE:    runRegister,
}

func runRegister(cmd *cobra.Command, args []string) error {
	return errors.New("register not implemented")
}
