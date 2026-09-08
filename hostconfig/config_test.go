package hostconfig

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
	t.Setenv("SHINZO_HOST_IDENTITY_SECRET", "shh")

	cfg, err := Load(writeToml(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.IdentitySecret != "shh" {
		t.Fatalf("expected env override to apply, got %q", cfg.Node.IdentitySecret)
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
