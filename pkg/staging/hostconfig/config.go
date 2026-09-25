package hostconfig

import (
	"errors"
	"io/fs"
	"os"

	"github.com/pelletier/go-toml/v2"
)

const configFileMode = 0o600

// Config is the parsed contents of a config.toml file.
type Config struct {
	Name string `toml:"name"`
}

// Create validates cfg, renders it, and saves it to path. Fails if a config
// already exists at path.
func Create(path string, cfg Config) error {
	if err := validate(cfg); err != nil {
		return err
	}

	data, err := render(cfg)
	if err != nil {
		return err
	}

	return save(path, data)
}

// Open reads the config at path and validates it before returning it.
func Open(path string) (Config, error) {
	cfg, err := load(path)
	if err != nil {
		return Config{}, err
	}

	if err := validate(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func load(path string) (Config, error) {
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

func save(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, configFileMode) //nolint:gosec // path is operator-supplied, not untrusted input.
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return errAlreadyExists
		}
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}

	return f.Close()
}
