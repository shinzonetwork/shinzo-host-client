package config

import (
	"os"
	"path/filepath"
)

func DefaultInstanceDir(name string) string {
	base := os.Getenv("XDG_DATA_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			home = "."
		}
		base = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(base, "shinzo-host", name)
}

func DefaultConfigPath(name string) string {
	return filepath.Join(DefaultInstanceDir(name), "config.toml")
}
