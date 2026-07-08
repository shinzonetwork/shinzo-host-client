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

	err := os.WriteFile(configPath, []byte(configContent), 0o600)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	expectedURL := "http://localhost:9181"
	// Test DefraDB config
	if cfg.DefraDB.URL != expectedURL {
		t.Errorf("Expected url '%s', got '%s'", expectedURL, cfg.DefraDB.URL)
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

	err := os.WriteFile(configPath, []byte(configContent), 0o600)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	// Set environment variables
	_ = os.Setenv("DEFRA_KEYRING_SECRET", "env_secret")
	defer func() { _ = os.Unsetenv("DEFRA_KEYRING_SECRET") }()

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.DefraDB.KeyringSecret != "original_secret" {
		t.Errorf("Expected keyring_secret 'original_secret', got '%s'", cfg.DefraDB.KeyringSecret)
	}
}

func TestLoadConfig_EmptyConfig(t *testing.T) {
	// Create a temporary config file with empty config
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "empty_config.yaml")

	configContent := `{}`

	err := os.WriteFile(configPath, []byte(configContent), 0o600)
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

	err := os.WriteFile(configPath, []byte(invalidContent), 0o600)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	_, err = LoadConfig(configPath)
	if err == nil {
		t.Error("Expected error for invalid YAML, got nil")
	}
}

func TestToInternalConfig(t *testing.T) {
	cfg := &Config{
		DefraDB: DefraDBConfig{
			URL:           "localhost:9181",
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

	appCfg := cfg.ToInternalConfig()
	require.NotNil(t, appCfg)
	require.Equal(t, "localhost:9181", appCfg.DefraDB.URL)
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

func TestToInternalConfig_Nil(t *testing.T) {
	var cfg *Config
	appCfg := cfg.ToInternalConfig()
	require.Nil(t, appCfg)
}

func TestLoadConfig_StartHeightEnvOverride(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("shinzo:\n  start_height: 100\n"), 0o600)
	require.NoError(t, err)

	t.Setenv("START_HEIGHT", "500")

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, uint64(500), cfg.Shinzo.StartHeight)
}

func TestLoadConfig_InvalidStartHeightEnv(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("shinzo:\n  start_height: 100\n"), 0o600)
	require.NoError(t, err)

	t.Setenv("START_HEIGHT", "not_a_number")

	_, err = LoadConfig(configPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid START_HEIGHT")
}

func TestLoadConfig_BootstrapPeersEnvOverride(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("defradb:\n  p2p:\n    bootstrap_peers: []\n"), 0o600)
	require.NoError(t, err)

	t.Setenv("BOOTSTRAP_PEERS", "peer1,peer2,peer3")

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, []string{"peer1", "peer2", "peer3"}, cfg.DefraDB.P2P.BootstrapPeers)
}

func TestLoadConfig_DefraURLEnvOverride(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("defradb:\n  url: localhost:9181\n"), 0o600)
	require.NoError(t, err)

	t.Setenv("DEFRA_URL", "0.0.0.0:9181")

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "0.0.0.0:9181", cfg.DefraDB.URL)
}

func TestLoadConfig_DefraURLEnvUnsetKeepsYAML(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("defradb:\n  url: localhost:9181\n"), 0o600)
	require.NoError(t, err)

	t.Setenv("DEFRA_URL", "")

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "localhost:9181", cfg.DefraDB.URL)
}

func TestLoadConfig_IndexerSchemaEndpoint_YAML(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("schema:\n  indexer_schema_endpoint: /custom/v2/schema\n"), 0o600)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "/custom/v2/schema", cfg.Schema.IndexerSchemaEndpoint)
}

func TestLoadConfig_IndexerSchemaEndpoint_EnvOverride(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("schema:\n  indexer_schema_endpoint: "+DefaultIndexerSchemaEndpoint+"\n"), 0o600)
	require.NoError(t, err)

	t.Setenv("INDEXER_SCHEMA_ENDPOINT", "/env/v3/schema")

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "/env/v3/schema", cfg.Schema.IndexerSchemaEndpoint)
}

func TestLoadConfig_IndexerSchemaEndpoint_Default(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("{}"), 0o600)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, DefaultIndexerSchemaEndpoint, cfg.Schema.IndexerSchemaEndpoint)
}

func TestLoadConfig_SchemaHTTPClientTimeout_YAML(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("schema:\n  http_client_timeout_secs: 60\n"), 0o600)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 60, cfg.Schema.HTTPClientTimeoutSecs)
}

func TestLoadConfig_SchemaHTTPClientTimeout_Default(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("{}"), 0o600)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 30, cfg.Schema.HTTPClientTimeoutSecs)
}

func TestLoadConfig_SchemaHTTPClientTimeout_Negative(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("schema:\n  http_client_timeout_secs: -5\n"), 0o600)
	require.NoError(t, err)

	_, err = LoadConfig(configPath)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNegativeSchemaTimeout)
}

func TestLoadConfig_SchemaHTTPClientTimeout_Excessive(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(configPath, []byte("schema:\n  http_client_timeout_secs: 999999\n"), 0o600)
	require.NoError(t, err)

	_, err = LoadConfig(configPath)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrExcessiveSchemaTimeout)
}

func TestLoadConfig_SchemaAuthToken(t *testing.T) {
	tests := []struct {
		name        string
		yamlContent string
		setEnv      func(t *testing.T)
		wantToken   string
	}{
		{
			name:        "env sets token",
			yamlContent: `{}`,
			setEnv:      func(t *testing.T) { t.Setenv("INDEXER_SCHEMA_ENDPOINT_AUTH_TOKEN", "my-secret-tok") },
			wantToken:   "my-secret-tok",
		},
		{
			name:        "env unset keeps empty default",
			yamlContent: `{}`,
			setEnv:      func(_ *testing.T) {},
			wantToken:   "",
		},
		{
			name:        "YAML auth_token field ignored due to yaml dash tag",
			yamlContent: "schema:\n  auth_token: \"should-be-ignored\"\n",
			setEnv:      func(_ *testing.T) {},
			wantToken:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			configPath := filepath.Join(tempDir, "config.yaml")

			err := os.WriteFile(configPath, []byte(tt.yamlContent), 0o600)
			require.NoError(t, err)

			tt.setEnv(t)

			cfg, err := LoadConfig(configPath)
			require.NoError(t, err)
			require.Equal(t, tt.wantToken, cfg.Schema.AuthToken)
		})
	}
}
