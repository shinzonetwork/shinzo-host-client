package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing test config: %v", err)
	}
	return path
}

func TestRunStart_MissingConfigFile(t *testing.T) {
	root := newRootCmd()
	root.SetArgs([]string{"start", "--config", filepath.Join(t.TempDir(), "does-not-exist.toml")})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected an error for a missing config file, got nil")
	}
	if !strings.Contains(err.Error(), "loading config") {
		t.Fatalf(`expected a "loading config" error, got: %v`, err)
	}
}

// startDefra is still a stub, so this exercises the whole chain up to
// there (config load, logger build, EnsureKeys, host.Start,
// hostserver.New) and stops exactly at the one piece that isn't real yet.
// data_dir is pinned to a temp dir so EnsureKeys creates its keyring
// there, not under this machine's real default instance directory.
func TestRunStart_ReachesHostStart(t *testing.T) {
	dataDir := t.TempDir()
	path := writeConfig(t, `
[http]
addr = ":0"

[node]
data_dir = "`+dataDir+`"
`)

	root := newRootCmd()
	root.SetArgs([]string{"start", "--config", path})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected an error since startDefra is still a stub, got nil")
	}
	if !strings.Contains(err.Error(), "starting defra") {
		t.Fatalf("expected the error to come from starting defra, got: %v", err)
	}
}
