package cli

import "testing"

func TestNewRootCmd_HasStartSubcommand(t *testing.T) {
	root := newRootCmd()

	var found bool
	for _, c := range root.Commands() {
		if c.Name() == "start" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal(`expected root command to have a "start" subcommand`)
	}
}

func TestNewRootCmd_ConfigFlagDefault(t *testing.T) {
	root := newRootCmd()

	flag := root.PersistentFlags().Lookup("config")
	if flag == nil {
		t.Fatal("expected a --config persistent flag")
	}
	if flag.DefValue != "" {
		t.Fatalf("expected --config default to be empty (resolved by config.Load), got %q", flag.DefValue)
	}
}
