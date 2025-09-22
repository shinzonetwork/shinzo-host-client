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
	if cfg.ShinzoAppConfig.DefraDB.Url != expectedUrl {
		t.Errorf("Expected url '%s', got '%s'", expectedUrl, cfg.ShinzoAppConfig.DefraDB.Url)
	}
	if cfg.ShinzoAppConfig.DefraDB.KeyringSecret != "test_secret" {
		t.Errorf("Expected keyring_secret 'test_secret', got '%s'", cfg.ShinzoAppConfig.DefraDB.KeyringSecret)
	}

	// Test P2P config
	if len(cfg.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers) != 2 {
		t.Errorf("Expected 2 bootstrap peers, got %d", len(cfg.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers))
	}
}

func TestLoadConfig_EmptyConfig(t *testing.T) {
	// Create a temporary config file with empty config
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "empty_config.yaml")

	configContent := `{}`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Test that empty config loads without error
	if cfg == nil {
		t.Error("Expected config to be loaded, got nil")
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
shinzo:
  minimum_attestations: "invalid yaml
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
