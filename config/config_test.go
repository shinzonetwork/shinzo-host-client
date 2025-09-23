package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_ValidYAML(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test_config.yaml")

	configContent := `
defradb:
  url: "http://localhost:9181"
  keyring_secret: "test_secret"
  p2p:
    enabled: true
    bootstrap_peers: ["peer1", "peer2"]
    listen_addr: "/ip4/0.0.0.0/tcp/9171"
  store:
    path: "/tmp/defra"

shinzohub:
  rpc_url: "some url"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	expectedUrl := "http://localhost:9181"
	// Test DefraDB config
	if cfg.DefraDB.Url != expectedUrl {
		t.Errorf("Expected url '%s', got '%s'", expectedUrl, cfg.DefraDB.Url)
	}
	if cfg.DefraDB.KeyringSecret != "test_secret" {
		t.Errorf("Expected keyring_secret 'test_secret', got '%s'", cfg.DefraDB.KeyringSecret)
	}

	// Test P2P config
	if len(cfg.DefraDB.P2P.BootstrapPeers) != 2 {
		t.Errorf("Expected 2 bootstrap peers, got %d", len(cfg.DefraDB.P2P.BootstrapPeers))
	}

	if len(cfg.ShinzoHub.RPCUrl) < 1 {
		t.Errorf("Expected a shinzohub rpc url")
	}
}

func TestLoadConfig_EnvironmentOverrides(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test_config.yaml")

	configContent := `
defradb:
  url: "http://localhost:9181"
  keyring_secret: "original_secret"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	// Set environment variables
	os.Setenv("DEFRA_KEYRING_SECRET", "env_secret")
	os.Setenv("DEFRA_URL", "someUrl")

	// Clean up environment variables after test
	defer func() {
		os.Unsetenv("DEFRA_KEYRING_SECRET")
		os.Unsetenv("DEFRA_URL")
	}()

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Verify environment overrides work
	if cfg.DefraDB.KeyringSecret != "env_secret" {
		t.Errorf("Expected keyring_secret 'env_secret', got '%s'", cfg.DefraDB.KeyringSecret)
	}
	if cfg.DefraDB.Url != "someUrl" {
		t.Errorf("Expected host 'someUrl', got '%s'", cfg.DefraDB.Url)
	}
}

func TestLoadConfig_InvalidPath(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Expected error for nonexistent config file, got nil")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	// Create a temporary config file with invalid YAML
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "invalid_config.yaml")

	invalidContent := `
defradb:
  url: "invalid yaml
`

	err := os.WriteFile(configPath, []byte(invalidContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	_, err = LoadConfig(configPath)
	if err == nil {
		t.Error("Expected error for invalid YAML, got nil")
	}
}
