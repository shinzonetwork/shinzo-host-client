package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func execute(args ...string) (string, error) {
	var buf bytes.Buffer

	cmd := rootCmd()

	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)

	err := cmd.Execute()

	return buf.String(), err
}

func TestRootHelpListsSubCommands(t *testing.T) {
	res, err := execute("--help")
	require.NoError(t, err, "--help return error")

	for _, subcommand := range []string{"init", "start", "register", "version"} {
		assert.Contains(t, res, subcommand, "--help is missing subcommand %s", subcommand)
	}
}

func TestRootRejectsUnknownSubCommand(t *testing.T) {
	_, err := execute("invalid")
	require.Error(t, err, "expected an error for unknown subcommand and got nil")
}
