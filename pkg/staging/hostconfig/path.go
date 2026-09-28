package hostconfig

import (
	"os"
	"path/filepath"
)

// DefaultPath returns the default config.toml location for name.
func DefaultPath(name string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(home, ".shinzo", "host", name, "config.toml"), nil
}
