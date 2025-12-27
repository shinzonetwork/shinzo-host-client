package config

import (
	"fmt"
	"os"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"gopkg.in/yaml.v3"
)

const CollectionName = "shinzo"

type Config struct {
	Shinzo          ShinzoConfig `yaml:"shinzo"`
	ShinzoAppConfig *config.Config
	HostConfig      HostConfig `yaml:"host"`
}

type ShinzoConfig struct {
	MinimumAttestations int    `yaml:"minimum_attestations"`
	RPCUrl              string `yaml:"rpc_url"`
	WebSocketUrl        string `yaml:"web_socket_url"`
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

	shinzoAppConfig, err := config.LoadConfig(path)
	if err != nil {
		return nil, err
	}
	cfg.ShinzoAppConfig = shinzoAppConfig

	return &cfg, nil
}
