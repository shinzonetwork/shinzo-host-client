package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig_ValidYAML(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test_config.yaml")

	configContent := `
defradb:
  url: "http://localhost:9181"
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

	// Test P2P config
	if len(cfg.DefraDB.P2P.BootstrapPeers) != 2 {
		t.Errorf("Expected 2 bootstrap peers, got %d", len(cfg.DefraDB.P2P.BootstrapPeers))
	}
}

func TestLoadConfig_EnvironmentOverrides(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test_config.yaml")

	configContent := `
defradb:
  keyring_secret: "original_secret"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	// Set environment variables
	os.Setenv("DEFRA_KEYRING_SECRET", "env_secret")
	defer os.Unsetenv("DEFRA_KEYRING_SECRET")

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Note: Environment overrides are now handled by the app-sdk config loader
	// The host config loads from YAML directly, so env vars don't apply here
	// This test now just verifies the YAML loading works
	if cfg.DefraDB.KeyringSecret != "original_secret" {
		t.Errorf("Expected keyring_secret 'original_secret', got '%s'", cfg.DefraDB.KeyringSecret)
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

func TestToAppConfig(t *testing.T) {
	cfg := &Config{
		DefraDB: DefraDBConfig{
			Url:           "localhost:9181",
			KeyringSecret: "secret123",
			P2P: DefraDBP2PConfig{
				Enabled:             true,
				BootstrapPeers:      []string{"peer1", "peer2"},
				ListenAddr:          "/ip4/0.0.0.0/tcp/9171",
				MaxRetries:          5,
				RetryBaseDelayMs:    100,
				ReconnectIntervalMs: 5000,
				EnableAutoReconnect: true,
			},
			Store: DefraDBStoreConfig{
				Path:                    "/tmp/defra",
				BlockCacheMB:            64,
				MemTableMB:              32,
				IndexCacheMB:            16,
				NumCompactors:           4,
				NumLevelZeroTables:      5,
				NumLevelZeroTablesStall: 15,
				ValueLogFileSizeMB:      256,
			},
		},
		Logger: LoggerConfig{
			Development: true,
		},
	}

	appCfg := cfg.ToAppConfig()
	require.NotNil(t, appCfg)
	require.Equal(t, "localhost:9181", appCfg.DefraDB.Url)
	require.Equal(t, "secret123", appCfg.DefraDB.KeyringSecret)
	require.True(t, appCfg.DefraDB.P2P.Enabled)
	// Bootstrap peers should be empty (added after ViewManager init)
	require.Empty(t, appCfg.DefraDB.P2P.BootstrapPeers)
	require.Equal(t, "/ip4/0.0.0.0/tcp/9171", appCfg.DefraDB.P2P.ListenAddr)
	require.Equal(t, 5, appCfg.DefraDB.P2P.MaxRetries)
	require.Equal(t, 100, appCfg.DefraDB.P2P.RetryBaseDelayMs)
	require.Equal(t, 5000, appCfg.DefraDB.P2P.ReconnectIntervalMs)
	require.True(t, appCfg.DefraDB.P2P.EnableAutoReconnect)
	require.Equal(t, "/tmp/defra", appCfg.DefraDB.Store.Path)
	require.Equal(t, int64(64), appCfg.DefraDB.Store.BlockCacheMB)
	require.Equal(t, int64(32), appCfg.DefraDB.Store.MemTableMB)
	require.Equal(t, int64(16), appCfg.DefraDB.Store.IndexCacheMB)
	require.Equal(t, 4, appCfg.DefraDB.Store.NumCompactors)
	require.Equal(t, 5, appCfg.DefraDB.Store.NumLevelZeroTables)
	require.Equal(t, 15, appCfg.DefraDB.Store.NumLevelZeroTablesStall)
	require.Equal(t, int64(256), appCfg.DefraDB.Store.ValueLogFileSizeMB)
	require.True(t, appCfg.Logger.Development)
}

func TestToAppConfig_Nil(t *testing.T) {
	var cfg *Config
	appCfg := cfg.ToAppConfig()
	require.Nil(t, appCfg)
}

func TestLoadConfig_StartHeightEnvOverride(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("shinzo:\n  start_height: 100\n"), 0644)
	require.NoError(t, err)

	t.Setenv("START_HEIGHT", "500")

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, uint64(500), cfg.Shinzo.StartHeight)
}

func TestLoadConfig_InvalidStartHeightEnv(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("shinzo:\n  start_height: 100\n"), 0644)
	require.NoError(t, err)

	t.Setenv("START_HEIGHT", "not_a_number")

	_, err = LoadConfig(configPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid START_HEIGHT")
}

func TestLoadConfig_BootstrapPeersEnvOverride(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("defradb:\n  p2p:\n    bootstrap_peers: []\n"), 0644)
	require.NoError(t, err)

	t.Setenv("BOOTSTRAP_PEERS", "peer1,peer2,peer3")

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, []string{"peer1", "peer2", "peer3"}, cfg.DefraDB.P2P.BootstrapPeers)
}
