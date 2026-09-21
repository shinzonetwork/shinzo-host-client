package hostconfig

import (
	"os"
	"regexp"

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

// Validate checks cfg for problems that would stop the host from starting.
func Validate(cfg Config) error {
	if cfg.Name == "" {
		return errEmptyName
	}

	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`).MatchString(cfg.Name) {
		return errInvalidName
	}

	return nil
}

// Save writes cfg to path as TOML.
func Save(path string, cfg Config) error {
	data, err := Render(cfg)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, configFileMode)
}
