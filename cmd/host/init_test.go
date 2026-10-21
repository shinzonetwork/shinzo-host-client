package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cosmos/go-bip39"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/keys"
)

func testMnemonic(t *testing.T) string {
	t.Helper()
	entropy, err := bip39.NewEntropy(128) //nolint:mnd // 128 bits -> 12 words
	require.NoError(t, err)
	mnemonic, err := bip39.NewMnemonic(entropy)
	require.NoError(t, err)
	return mnemonic
}

// writeRecoverFile writes content to a fresh file and returns its path,
// for tests exercising --recover-file.
func writeRecoverFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mnemonic.txt")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

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

// String overrides are covered above. A bool override goes through a
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

// Prepare's own validate call should still reject a bad override reaching
// it through the CLI, same as it would for a bad value written by hand.
func TestRunInitRejectsInvalidOverride(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("logger.level", "bogus"))
	cmd.SetOut(&bytes.Buffer{})

	require.Error(t, runInit(cmd, nil))
	require.NoFileExists(t, filepath.Join(home, "config.toml"))
}

// The output isn't just the one "Initialized at" line. printConfig's
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

// --data-dir is never saved to config.toml. init should remind the
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

func TestRunInitGeneratesKeys(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.FileExists(t, filepath.Join(home, "keys", "node.key"))
	require.Contains(t, out.String(), "Operator address: shinzo1")
}

func TestRunInitRecoverUsesGivenMnemonic(t *testing.T) {
	mnemonic := testMnemonic(t)
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", writeRecoverFile(t, mnemonic)))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))

	// Derive independently from the same mnemonic, in a separate dir, and
	// confirm it's the same operator address init just reported.
	want, _, err := keys.EnsureKeys(filepath.Join(t.TempDir(), "keys"), mnemonic, true, zap.NewNop().Sugar())
	require.NoError(t, err)
	wantAddr, err := want.ShinzoAddress()
	require.NoError(t, err)

	require.Contains(t, out.String(), wantAddr)
}

// init must not generate and print a mnemonic when one was recovered.
func TestRunInitDoesNotPrintAGeneratedMnemonicWhenRecovering(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", writeRecoverFile(t, testMnemonic(t))))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.NotContains(t, out.String(), "Generated a new node seed phrase")
}

// init prints the freshly generated mnemonic exactly once, so the
// operator has a chance to record it before it's gone for good.
func TestRunInitPrintsGeneratedMnemonicWhenNotRecovering(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.Contains(t, out.String(), "Generated a new node seed phrase")
}

// A bad --recover-file is checked before anything else, so a failed
// init leaves nothing behind, no config.toml, no directories. Without
// that check up front, Prepare would have already created the
// directories by the time EnsureKeys got around to rejecting the
// mnemonic.
func TestRunInitRejectsInvalidRecoverMnemonic(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", writeRecoverFile(t, "not a real mnemonic at all")))
	cmd.SetOut(&bytes.Buffer{})

	require.Error(t, runInit(cmd, nil))
	require.NoDirExists(t, home)
}

func TestRunInitRejectsMissingRecoverFile(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", filepath.Join(t.TempDir(), "does-not-exist.txt")))
	cmd.SetOut(&bytes.Buffer{})

	require.Error(t, runInit(cmd, nil))
	require.NoDirExists(t, home)
}

// Commit (the step that writes config.toml) only ever runs after
// EnsureKeys succeeds, so a mismatched recover value never reaches it.
// Without that ordering, a config.toml written before the mismatch was
// caught would block every retry, even a correct plain init, with
// "config already exists", without ever reaching the key check again.
func TestRunInitLeavesNoConfigBehindOnRecoverMismatch(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedKeys(t, home) // pre-existing keys, as if config.toml alone was reset by hand

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", writeRecoverFile(t, testMnemonic(t))))
	cmd.SetOut(&bytes.Buffer{})

	require.Error(t, runInit(cmd, nil))
	require.NoFileExists(t, filepath.Join(home, "config.toml"))

	// A plain, correct retry must now succeed.
	retry := initCmd()
	require.NoError(t, retry.Flags().Set("home", home))
	retry.SetOut(&bytes.Buffer{})
	require.NoError(t, runInit(retry, nil))
}
