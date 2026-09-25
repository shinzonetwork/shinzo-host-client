package hostconfig

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigCanSaveTOML(t *testing.T) {
	cfg := Config{Name: "host1"}
	data, err := render(cfg)
	require.NoError(t, err)

	require.NoError(t, save(filepath.Join(t.TempDir(), "config.toml"), data))
}

func TestConfigSaveWriteFileToDir(t *testing.T) {
	cfg := Config{Name: "host1"}
	path := filepath.Join(t.TempDir(), "config.toml")

	data, err := render(cfg)
	require.NoError(t, err)

	require.NoError(t, save(path, data))
	require.FileExists(t, path)
}

func TestSaveWritesExactBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	want := []byte("arbitrary content")

	require.NoError(t, save(path, want))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestSaveFailsIfAlreadyExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")

	require.NoError(t, save(path, []byte("first")))

	err := save(path, []byte("second"))
	require.ErrorIs(t, err, errAlreadyExists)
}

func TestSaveFailsIfDirMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-dir", "config.toml")

	err := save(path, []byte("data"))
	require.Error(t, err)
	require.NotErrorIs(t, err, errAlreadyExists)
}

func TestLoadValidTOML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`name = "host1"`), 0o600))

	cfg, err := load(path)
	require.NoError(t, err)
	require.Equal(t, "host1", cfg.Name)
}

func TestLoadMissingFile(t *testing.T) {
	_, err := load(filepath.Join(t.TempDir(), "missing.toml"))
	require.Error(t, err)
}

func TestLoadMalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed.toml")
	require.NoError(t, os.WriteFile(path, []byte(`not = [ valww`), 0o600))

	_, err := load(path)
	require.Error(t, err)
}

func TestConfigRoundTrip(t *testing.T) {
	cfg := Config{Name: "host1"}
	path := filepath.Join(t.TempDir(), "config.toml")

	data, err := render(cfg)
	require.NoError(t, err)
	require.NoError(t, save(path, data))

	loadedCfg, err := load(path)
	require.NoError(t, err)

	require.Equal(t, cfg, loadedCfg)
}

func TestCreateBuildsAndSavesConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")

	err := Create(path, Config{Name: "host1"})
	require.NoError(t, err)
	require.FileExists(t, path)
}

func TestCreateFailsIfAlreadyExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")

	err := Create(path, Config{Name: "host1"})
	require.NoError(t, err)

	err = Create(path, Config{Name: "host1"})
	require.ErrorIs(t, err, errAlreadyExists)
}

func TestCreateRejectsInvalidName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")

	err := Create(path, Config{Name: ""})
	require.Error(t, err)
	require.NoFileExists(t, path)
}

func TestOpenReadsExistingConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`name = "host1"`), 0o600))

	cfg, err := Open(path)
	require.NoError(t, err)
	require.Equal(t, "host1", cfg.Name)
}

func TestOpenFailsIfMissing(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "missing.toml"))
	require.Error(t, err)
}

func TestOpenRejectsInvalidStoredName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`name = ""`), 0o600))

	_, err := Open(path)
	require.Error(t, err)
}

func TestCreateThenOpenRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	cfg := Config{Name: "host1"}

	err := Create(path, cfg)
	require.NoError(t, err)

	opened, err := Open(path)
	require.NoError(t, err)

	require.Equal(t, cfg, opened)
}
