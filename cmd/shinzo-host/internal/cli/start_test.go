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

// ephemeral port so this doesn't fight anything else on the machine for
// :8080, and the short timeout stands in for a real shutdown signal
func TestRunStart_ReachesHostStart(t *testing.T) {
	path := writeConfig(t, `
[http]
addr = ":0"
`)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond) //nolint:mnd
	defer cancel()

	root := newRootCmd()
	root.SetArgs([]string{"start", "--config", path})
	root.SetContext(ctx)

	if err := root.Execute(); err != nil {
		t.Fatalf("expected a clean shutdown, got: %v", err)
	}
}
