package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

const CollectionName = "shinzo"

type Config struct {
	DefraDB struct {
		Host          string `yaml:"host"`
		Port          int    `yaml:"port"`
		KeyringSecret string `yaml:"keyring_secret"`
		P2P           struct {
			Enabled        bool     `yaml:"enabled"`
			BootstrapPeers []string `yaml:"bootstrap_peers"`
			ListenAddr     string   `yaml:"listen_addr"`
		} `yaml:"p2p"`
		Store struct {
			Path string `yaml:"path"`
		} `yaml:"store"`
	} `yaml:"defradb"`

	ShinzoHub struct {
		RPCUrl string `yaml:"rpc_url"`
	} `yaml:"shinzohub"`

	Logger struct {
		Development bool `yaml:"development"`
	} `yaml:"logger"`
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

	if port := os.Getenv("DEFRA_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.DefraDB.Port = p
		}
	}

	if host := os.Getenv("DEFRA_HOST"); host != "" {
		cfg.DefraDB.Host = host
	}

	return &cfg, nil
}
