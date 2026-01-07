package config

import (
	"fmt"
	"os"

	appConfig "github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"gopkg.in/yaml.v3"
)

const CollectionName = "shinzo"

// DefraDBP2PConfig represents P2P configuration for DefraDB
type DefraDBP2PConfig struct {
	Enabled             bool     `yaml:"enabled"`
	BootstrapPeers      []string `yaml:"bootstrap_peers"`
	ListenAddr          string   `yaml:"listen_addr"`
	MaxRetries          int      `yaml:"max_retries"`
	RetryBaseDelayMs    int      `yaml:"retry_base_delay_ms"`
	ReconnectIntervalMs int      `yaml:"reconnect_interval_ms"`
	EnableAutoReconnect bool     `yaml:"enable_auto_reconnect"`
}

// DefraDBStoreConfig represents store configuration for DefraDB
type DefraDBStoreConfig struct {
	Path string `yaml:"path"`
}

// DefraDBConfig represents DefraDB configuration
type DefraDBConfig struct {
	Url           string             `yaml:"url"`
	KeyringSecret string             `yaml:"keyring_secret"`
	P2P           DefraDBP2PConfig   `yaml:"p2p"`
	Store         DefraDBStoreConfig `yaml:"store"`
}

// LoggerConfig represents logger configuration
type LoggerConfig struct {
	Development bool `yaml:"development"`
}

type Config struct {
	DefraDB    DefraDBConfig `yaml:"defradb"`
	Shinzo     ShinzoConfig  `yaml:"shinzo"`
	Logger     LoggerConfig  `yaml:"logger"`
	HostConfig HostConfig    `yaml:"host"`
}

type ShinzoConfig struct {
	MinimumAttestations int    `yaml:"minimum_attestations"`
	HubBaseURL          string `yaml:"hub_base_url"`
	StartHeight         uint64 `yaml:"start_height"`

	// P2P Control Settings
	P2PEnabled bool `yaml:"p2p_enabled"`

	// View Management Settings
	ViewInactivityTimeout string `yaml:"view_inactivity_timeout"` // Stop updating after inactivity (default: 24h)
	ViewCleanupInterval   string `yaml:"view_cleanup_interval"`   // Check for inactive views (default: 1h)
	ViewWorkerCount       int    `yaml:"view_worker_count"`       // Workers for lens transformations (default: 2)
	ViewQueueSize         int    `yaml:"view_queue_size"`         // Queue size for view processing jobs (default: 1000)

	// Message Cache Settings
	CacheSize          int `yaml:"cache_size"`            // Maximum number of messages in cache
	CacheQueueSize     int `yaml:"cache_queue_size"`      // Size of processing queue
	CacheMaxAgeSeconds int `yaml:"cache_max_age_seconds"` // Maximum age of cached messages
	WorkerCount        int `yaml:"worker_count"`          // Number of worker goroutines
}

type HostConfig struct {
	LensRegistryPath string `yaml:"lens_registry_path"` // At this path, we will store the lens' wasm files
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	// Load YAML config
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// ToAppConfig converts the host config to an app-sdk config
func (c *Config) ToAppConfig() *appConfig.Config {
	if c == nil {
		return nil
	}

	return &appConfig.Config{
		DefraDB: appConfig.DefraDBConfig{
			Url:           c.DefraDB.Url,
			KeyringSecret: c.DefraDB.KeyringSecret,
			P2P: appConfig.DefraP2PConfig{
				Enabled:             c.DefraDB.P2P.Enabled,
				BootstrapPeers:      c.DefraDB.P2P.BootstrapPeers,
				ListenAddr:          c.DefraDB.P2P.ListenAddr,
				MaxRetries:          c.DefraDB.P2P.MaxRetries,
				RetryBaseDelayMs:    c.DefraDB.P2P.RetryBaseDelayMs,
				ReconnectIntervalMs: c.DefraDB.P2P.ReconnectIntervalMs,
				EnableAutoReconnect: c.DefraDB.P2P.EnableAutoReconnect,
			},
			Store: appConfig.DefraStoreConfig{
				Path: c.DefraDB.Store.Path,
			},
		},
		Logger: appConfig.LoggerConfig{
			Development: c.Logger.Development,
		},
	}
}
