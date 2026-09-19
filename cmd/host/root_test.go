package main

import (
	"bytes"
	"strings"
	"testing"
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
	if err != nil {
		t.Fatalf("--help return error %v", err)
	}

	for _, subcommand := range []string{"init", "start", "register", "version"} {
		if !strings.Contains(res, subcommand) {
			t.Errorf("--help is missing subcommand %s", subcommand)
		}
	}
}

func TestRootRejectsUnknownSubCommand(t *testing.T) {
	if _, err := execute("invalid"); err == nil {
		t.Fatalf("expected an error for unknown subcommand and got nil")
	}
}
