package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeToml(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing test config: %v", err)
	}
	return path
}

func TestLoad_EmptyFileUsesDefaults(t *testing.T) {
	cfg, err := Load(writeToml(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Fatalf("expected default http.addr :8080, got %q", cfg.HTTP.Addr)
	}
	if cfg.Node.DataDir == "" {
		t.Fatal("expected DataDir to be derived when left blank")
	}
	if cfg.Node.KeyDir != filepath.Join(cfg.Node.DataDir, "keys") {
		t.Fatalf("expected KeyDir to default to <data_dir>/keys, got %q", cfg.Node.KeyDir)
	}
	if cfg.Node.KeyringPassword == "" {
		t.Fatal("expected a default keyring password")
	}
}

func TestLoad_KeyDirOverride(t *testing.T) {
	cfg, err := Load(writeToml(t, `
[node]
key_dir = "/custom/keys"
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.KeyDir != "/custom/keys" {
		t.Fatalf("expected an explicit key_dir to override the default, got %q", cfg.Node.KeyDir)
	}
}

func TestLoad_OverridesOnlyWhatItMentions(t *testing.T) {
	cfg, err := Load(writeToml(t, `
[http]
addr = ":9999"
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTP.Addr != ":9999" {
		t.Fatalf("expected http.addr override to apply, got %q", cfg.HTTP.Addr)
	}
	if cfg.Shinzo.HubBaseURL != "testnet.shinzo.network" {
		t.Fatalf("expected unrelated defaults to survive, got hub_base_url=%q", cfg.Shinzo.HubBaseURL)
	}
}

func TestLoad_ACPSection(t *testing.T) {
	cfg, err := Load(writeToml(t, `
[shinzo]
chain_id = 12345

[acp]
enabled = true
min_query_balance = "1000000"
epoch_length = 100
as_base_url = "https://accounting.internal"
attester_window = "5m"
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.ACP.Enabled {
		t.Fatal("expected acp.enabled to be true")
	}
	if cfg.Shinzo.ChainID != 12345 {
		t.Fatalf("expected shinzo.chain_id 12345, got %d", cfg.Shinzo.ChainID)
	}
	if cfg.ACP.AttesterWindow != "5m" {
		t.Fatalf("expected attester_window %q, got %q", "5m", cfg.ACP.AttesterWindow)
	}
}

func TestLoad_PrunerSection(t *testing.T) {
	cfg, err := Load(writeToml(t, `
[pruner]
enabled = true
max_blocks = 5000
docs_per_block = 1200
interval_seconds = 45
prune_history = true
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Pruner.Enabled {
		t.Fatal("expected pruner.enabled to be true")
	}
	if cfg.Pruner.MaxBlocks != 5000 {
		t.Fatalf("expected pruner.max_blocks 5000, got %d", cfg.Pruner.MaxBlocks)
	}
	if cfg.Pruner.DocsPerBlock != 1200 {
		t.Fatalf("expected pruner.docs_per_block 1200, got %d", cfg.Pruner.DocsPerBlock)
	}
	if cfg.Pruner.IntervalSeconds != 45 {
		t.Fatalf("expected pruner.interval_seconds 45, got %d", cfg.Pruner.IntervalSeconds)
	}
	if !cfg.Pruner.PruneHistory {
		t.Fatal("expected pruner.prune_history to be true")
	}
}

func TestLoad_PrunerDefaultsToDisabled(t *testing.T) {
	cfg, err := Load(writeToml(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Pruner.Enabled {
		t.Fatal("expected pruner.enabled to default to false")
	}
}

func TestLoad_ACPDefaultsToDisabled(t *testing.T) {
	cfg, err := Load(writeToml(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ACP.Enabled {
		t.Fatal("expected acp.enabled to default to false")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.toml"))
	if err == nil {
		t.Fatal("expected an error for a missing file, got nil")
	}
}

func TestLoad_InvalidEventFilterMode(t *testing.T) {
	_, err := Load(writeToml(t, `
[event_filter]
enabled = true
mode = "not-a-real-mode"
`))
	if err == nil {
		t.Fatal("expected validation to reject an invalid event_filter.mode, got nil")
	}
}

func TestLoad_EnvOverridesSecrets(t *testing.T) {
	t.Setenv("SHINZO_HOST_SCHEMA_AUTH_TOKEN", "shh")

	cfg, err := Load(writeToml(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Schema.AuthToken != "shh" {
		t.Fatalf("expected env override to apply, got %q", cfg.Schema.AuthToken)
	}
}

func TestLoad_EmptyPathBootstrapsDefaultConfig(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())

	wantPath := DefaultConfigPath("default")
	if _, err := os.Stat(wantPath); !os.IsNotExist(err) {
		t.Fatalf("expected %s not to exist yet", wantPath)
	}

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Fatalf("expected default http.addr :8080, got %q", cfg.HTTP.Addr)
	}

	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("expected Load to create %s, got: %v", wantPath, err)
	}
}

func TestLoad_ExplicitMissingPathIsNotBootstrapped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.toml")

	if _, err := Load(path); err == nil {
		t.Fatal("expected an error, got nil")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("expected the explicit path to still not exist after a failed Load")
	}
}

func TestDefaultConfigPath_RespectsXDGDataHome(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", "/tmp/xdg-data-home")

	got := DefaultConfigPath("default")
	want := filepath.Join("/tmp/xdg-data-home", "shinzo-host", "default", "config.toml")
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
