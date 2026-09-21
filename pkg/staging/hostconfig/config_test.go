package hostconfig

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
)

func TestConfigCanSaveTOML(t *testing.T) {
	cfg := Config{Name: "host1"}
	require.NoError(t, Save(filepath.Join(t.TempDir(), "config.toml"), cfg))
}

func TestConfigSaveWriteFileToDir(t *testing.T) {
	cfg := Config{Name: "host1"}
	path := filepath.Join(t.TempDir(), "config.toml")

	require.NoError(t, Save(path, cfg))
	require.FileExists(t, path)
}

func TestLoadValidTOML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`name = "host1"`), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "host1", cfg.Name)
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.toml"))
	require.Error(t, err)
}

func TestLoadMalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed.toml")
	require.NoError(t, os.WriteFile(path, []byte(`not = [ valww`), 0o600))

	_, err := Load(path)
	require.Error(t, err)
}

func TestSaveWritesCorrectBytes(t *testing.T) {
	cfg := Config{Name: "host1"}
	path := filepath.Join(t.TempDir(), "config.toml")

	require.NoError(t, Save(path, cfg))

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var newCfg Config
	require.NoError(t, toml.Unmarshal(data, &newCfg))
	require.Equal(t, cfg.Name, newCfg.Name)
}

func TestConfigRoundTrip(t *testing.T) {
	cfg := Config{Name: "host1"}
	path := filepath.Join(t.TempDir(), "config.toml")

	require.NoError(t, Save(path, cfg))

	loadedCfg, err := Load(path)
	require.NoError(t, err)

	require.Equal(t, cfg, loadedCfg)
}
