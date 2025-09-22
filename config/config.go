package config

import (
	"fmt"
	"os"

	"github.com/shinzonetwork/app-sdk/pkg/config"
	"gopkg.in/yaml.v3"
)

const CollectionName = "shinzo"

type Config struct {
	Shinzo          ShinzoConfig `yaml:"shinzo"`
	ShinzoAppConfig *config.Config
}

type ShinzoConfig struct {
	MinimumAttestations int `yaml:"minimum_attestations"`
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
