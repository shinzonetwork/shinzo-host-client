package hostconfig

import (
	"os"
	"path/filepath"
)

// Directory resolution
//
// These functions resolve on disk paths for the host, applying defaults
// when a value isn't supplied. Add a new resolver here for any new
// directory that needs the same "use override or fall back to a default
// under home" behavior.

const (
	configFileName = "config.toml"
	homeDirName    = ".shinzo/host"
	dataDirName    = "data"
	keyDirName     = "keys"
	filterDirName  = "filter"
)

func resolveHome(home string) (string, error) {
	if home == "" {
		root, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}

		home = filepath.Join(root, homeDirName)
	}

	return filepath.Abs(home)
}

func resolveDataDir(home, dataDir string) (string, error) {
	if dataDir == "" {
		return filepath.Join(home, dataDirName), nil
	}

	return filepath.Abs(dataDir)
}

func resolveConfigPath(home, configPath string) (string, error) {
	if configPath == "" {
		return filepath.Join(home, configFileName), nil
	}

	return filepath.Abs(configPath)
}

// Custom resolves
//
// No override: these are always just a fixed subdir of home. So, if
// anyone wants to create a new dir, this is where you add it.

func resolveKeyDir(home string) string {
	return filepath.Join(home, keyDirName)
}

func resolveFilterDir(home string) string {
	return filepath.Join(home, filterDirName)
}

// resolve computes every directory/path the host needs and returns a
// fully populated Config. Each field is resolved independently via its
// resolve* helper, using resolvedHome (not the raw home arg) as the base
// so overrides and defaults stay consistent with each other.
func resolve(home, dataDir, configPath string) (Config, error) {
	resolvedHome, err := resolveHome(home)
	if err != nil {
		return Config{}, err
	}

	resolvedDataDir, err := resolveDataDir(resolvedHome, dataDir)
	if err != nil {
		return Config{}, err
	}

	resolvedConfigPath, err := resolveConfigPath(resolvedHome, configPath)
	if err != nil {
		return Config{}, err
	}

	return Config{
		Home:       resolvedHome,
		DataDir:    resolvedDataDir,
		ConfigPath: resolvedConfigPath,
		KeyDir:     resolveKeyDir(resolvedHome),
		FilterDir:  resolveFilterDir(resolvedHome),
	}, nil
}

// createPath ensures all directories the host needs exist on disk,
// creating any that are missing. Everything here is read straight off cfg,
// since resolve has already computed it all. Add new required dirs here as
// a new entry in dirs; if resolve grows a new field for it, use that field
// rather than recomputing it with its own resolve* helper, so there's only
// ever one place that formula lives.
func createPath(cfg Config) error {
	dirs := []string{
		cfg.Home,
		cfg.DataDir,
		cfg.KeyDir,
		cfg.FilterDir,
		filepath.Dir(cfg.ConfigPath),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, dirMode); err != nil {
			return err
		}
	}

	return nil
}
