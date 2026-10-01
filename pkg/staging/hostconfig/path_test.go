package hostconfig

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveHomeDefaultsToFixedPath(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)

	home, err := resolveHome("")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(fakeHome, ".shinzo", "host"), home)
}

func TestResolveHomeUsesGivenValue(t *testing.T) {
	given := filepath.Join(t.TempDir(), "myhost")

	home, err := resolveHome(given)
	require.NoError(t, err)
	require.Equal(t, given, home)
}

func TestResolveHomeMakesRelativeValueAbsolute(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	home, err := resolveHome("relative-home")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(wd, "relative-home"), home)
}

func TestResolveDataDirDefaultsUnderHome(t *testing.T) {
	dataDir, err := resolveDataDir("/resolved/home", "")
	require.NoError(t, err)
	require.Equal(t, filepath.Join("/resolved/home", "data"), dataDir)
}

func TestResolveDataDirUsesGivenValue(t *testing.T) {
	given := filepath.Join(t.TempDir(), "bigdisk")

	dataDir, err := resolveDataDir("/resolved/home", given)
	require.NoError(t, err)
	require.Equal(t, given, dataDir)
}

func TestResolveDataDirMakesRelativeValueAbsolute(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	dataDir, err := resolveDataDir("/resolved/home", "relative-data")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(wd, "relative-data"), dataDir)
}

func TestResolveConfigPathDefaultsUnderHome(t *testing.T) {
	got, err := resolveConfigPath("/resolved/home", "")
	require.NoError(t, err)
	require.Equal(t, filepath.Join("/resolved/home", "config.toml"), got)
}

func TestResolveConfigPathUsesGivenValue(t *testing.T) {
	given := filepath.Join(t.TempDir(), "myhost", "config.toml")

	got, err := resolveConfigPath("/resolved/home", given)
	require.NoError(t, err)
	require.Equal(t, given, got)
}

func TestResolveConfigPathMakesRelativeValueAbsolute(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	got, err := resolveConfigPath("/resolved/home", "relative/config.toml")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(wd, "relative", "config.toml"), got)
}

func TestResolveKeyDir(t *testing.T) {
	require.Equal(t, filepath.Join("/resolved/home", "keys"), resolveKeyDir("/resolved/home"))
}

func TestResolveFilterDir(t *testing.T) {
	require.Equal(t, filepath.Join("/resolved/home", "filter"), resolveFilterDir("/resolved/home"))
}

func TestResolveBuildsCompleteConfig(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)

	cfg, err := resolve("", "", "")
	require.NoError(t, err)

	wantHome := filepath.Join(fakeHome, ".shinzo", "host")
	require.Equal(t, wantHome, cfg.Home)
	require.Equal(t, filepath.Join(wantHome, "data"), cfg.DataDir)
	require.Equal(t, filepath.Join(wantHome, "config.toml"), cfg.ConfigPath)
	require.Equal(t, filepath.Join(wantHome, "keys"), cfg.KeyDir)
	require.Equal(t, filepath.Join(wantHome, "filter"), cfg.FilterDir)
}

func TestResolveDoesNotSetPersistedFields(t *testing.T) {
	cfg, err := resolve(filepath.Join(t.TempDir(), "myhost"), "", "")
	require.NoError(t, err)

	require.Equal(t, LoggerConfig{}, cfg.Logger, "resolve should leave persisted fields untouched, that's applyDefaults'/load's job")
	require.Equal(t, HTTPConfig{}, cfg.HTTP)
}

func TestCreatePathCreatesEveryDir(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	cfg, err := resolve(home, "", "")
	require.NoError(t, err)

	require.NoError(t, createPath(cfg))

	require.DirExists(t, cfg.Home)
	require.DirExists(t, cfg.DataDir)
	require.DirExists(t, cfg.KeyDir)
	require.DirExists(t, cfg.FilterDir)
	require.DirExists(t, filepath.Dir(cfg.ConfigPath))
}

func TestCreatePathIsIdempotent(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	cfg, err := resolve(home, "", "")
	require.NoError(t, err)

	require.NoError(t, createPath(cfg))
	require.NoError(t, createPath(cfg))
}

func TestCreatePathHandlesCustomDataDirOutsideHome(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	dataDir := filepath.Join(t.TempDir(), "bigdisk")

	cfg, err := resolve(home, dataDir, "")
	require.NoError(t, err)

	require.NoError(t, createPath(cfg))
	require.DirExists(t, dataDir)
}
