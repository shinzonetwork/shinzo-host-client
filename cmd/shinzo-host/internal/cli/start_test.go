package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestRunStart_ReachesHostStart(t *testing.T) {
	dataDir := t.TempDir()
	path := writeConfig(t, `
[http]
addr = ":0"

[p2p]
enabled = false

[node]
data_dir = "`+dataDir+`"
`)

	root := newRootCmd()
	root.SetArgs([]string{"start", "--config", path})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := root.ExecuteContext(ctx); err != nil {
		t.Fatalf("expected a clean start and shutdown, got: %v", err)
	}
}
