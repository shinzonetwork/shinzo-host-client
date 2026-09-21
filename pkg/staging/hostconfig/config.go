package hostconfig

import (
	"os"

	"github.com/pelletier/go-toml/v2"
)

const configFileMode = 0o600

// Config is the parsed contents of a config.toml file.
type Config struct {
	Name string `toml:"name"`
}

// Load reads and parses a config file at path.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is operator-supplied, not untrusted input.
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Save writes cfg to path as TOML.
func Save(path string, cfg Config) error {
	data, err := Render(cfg)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, configFileMode)
}
