package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

const CollectionName = "shinzo"

type Config struct {
	DefraDB   DefraDBConfig   `yaml:"defradb"`
	ShinzoHub ShinzoHubConfig `yaml:"shinzohub"`
	Logger    LoggerConfig    `yaml:"logger"`
}

type DefraDBConfig struct {
	Url           string           `yaml:"url"`
	KeyringSecret string           `yaml:"keyring_secret"`
	P2P           DefraP2PConfig   `yaml:"p2p"`
	Store         DefraStoreConfig `yaml:"store"`
}

type DefraP2PConfig struct {
	Enabled        bool     `yaml:"enabled"`
	BootstrapPeers []string `yaml:"bootstrap_peers"`
	ListenAddr     string   `yaml:"listen_addr"`
}

type DefraStoreConfig struct {
	Path string `yaml:"path"`
}

type ShinzoHubConfig struct {
	RPCUrl string `yaml:"rpc_url"`
}

type LoggerConfig struct {
	Development bool `yaml:"development"`
}

// LoadConfig loads configuration from a YAML file and environment variables
func LoadConfig(path string) (*Config, error) {
	// Load .env file if it exists
	_ = godotenv.Load()

	// Load YAML config
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Override with environment variables
	if keyringSecret := os.Getenv("DEFRA_KEYRING_SECRET"); keyringSecret != "" {
		cfg.DefraDB.KeyringSecret = keyringSecret
	}

	if url := os.Getenv("DEFRA_URL"); url != "" {
		cfg.DefraDB.Url = url
	}

	return &cfg, nil
}
