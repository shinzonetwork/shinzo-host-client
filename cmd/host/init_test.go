package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
)

func TestRunInitCreatesInstance(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.FileExists(t, filepath.Join(home, "config.toml"))
	require.Contains(t, out.String(), home)
}

func TestRunInitFailsIfAlreadyInitialized(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	cmd.SetOut(&bytes.Buffer{})
	require.NoError(t, runInit(cmd, nil))

	cmd = initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	cmd.SetOut(&bytes.Buffer{})
	require.Error(t, runInit(cmd, nil))
}

func TestRunInitBakesOverridesIntoSavedFile(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("logger.level", "warn"))
	cmd.SetOut(&bytes.Buffer{})

	require.NoError(t, runInit(cmd, nil))

	cfg, err := hostconfig.Load(home, "", "", nil)
	require.NoError(t, err)
	require.Equal(t, "warn", cfg.Logger.Level)
}

// String overrides are covered above; a bool override goes through a
// different branch of getOverrides (GetBool, not GetString), so it needs
// its own check that it actually reaches the saved file too.
func TestRunInitBakesBoolOverrideIntoSavedFile(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("logger.development", "true"))
	cmd.SetOut(&bytes.Buffer{})

	require.NoError(t, runInit(cmd, nil))

	cfg, err := hostconfig.Load(home, "", "", nil)
	require.NoError(t, err)
	require.True(t, cfg.Logger.Development)
}

func TestRunInitUsesCustomDataDir(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	dataDir := filepath.Join(t.TempDir(), "bigdisk")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("data-dir", dataDir))
	cmd.SetOut(&bytes.Buffer{})

	require.NoError(t, runInit(cmd, nil))
	require.DirExists(t, dataDir)
}

// Setup's own validate call should still reject a bad override reaching it
// through the CLI, same as it would for a bad value written by hand.
func TestRunInitRejectsInvalidOverride(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("logger.level", "bogus"))
	cmd.SetOut(&bytes.Buffer{})

	require.Error(t, runInit(cmd, nil))
	require.NoFileExists(t, filepath.Join(home, "config.toml"))
}

// The output isn't just the one "Initialized at" line; printConfig's
// output should show up too, with the actual resolved/overridden values.
func TestRunInitPrintsFullResolvedConfig(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("http.addr", ":9090"))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.Contains(t, out.String(), filepath.Join(home, "data"))
	require.Contains(t, out.String(), "addr=:9090")
}

// --data-dir is never saved to config.toml; init should remind the
// operator of that right when they set it, so they don't discover it the
// hard way on a later start.
func TestRunInitWarnsWhenDataDirSet(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	dataDir := filepath.Join(t.TempDir(), "bigdisk")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("data-dir", dataDir))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.Contains(t, out.String(), "not saved")
}

func TestRunInitDoesNotWarnWhenDataDirUnset(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.NotContains(t, out.String(), "not saved")
}
