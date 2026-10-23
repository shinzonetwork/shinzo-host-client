package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/cosmos/go-bip39"
	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
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

// Running init again on an already-initialized host is idempotent,
// not an error: it reports what's already there and leaves it alone,
// rather than erroring or generating a new identity.
func TestRunInitOnAlreadyInitializedHostIsIdempotent(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	var first bytes.Buffer
	cmd.SetOut(&first)
	require.NoError(t, runInit(cmd, nil))

	cmd = initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	var second bytes.Buffer
	cmd.SetOut(&second)
	require.NoError(t, runInit(cmd, nil))

	require.Contains(t, second.String(), "Already initialized")
	require.NotContains(t, second.String(), "Generated a new node seed phrase", "re-running init must never generate a new identity")

	// the operator address reported both times must be identical,
	// confirming the second run loaded the same keys, not fresh ones
	addrLine := regexp.MustCompile(`Operator address: \S+`).FindString(first.String())
	require.NotEmpty(t, addrLine)
	require.Contains(t, second.String(), addrLine)
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
	require.False(t, hostkey.Exist(filepath.Join(home, "keys")), "a bad override must be caught before keys are ever touched")
}

// A bad override combined with --recover-file must be caught before
// keys are recovered and stored. Without that, a first (failed) run
// stores the recovered keys anyway, and a retry with the typo fixed
// then fails differently: keys already exist, nothing to recover,
// even though the overall command never actually succeeded once.
func TestRunInitRejectsInvalidOverrideBeforeRecoveringKeys(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	mnemonic := testMnemonic(t)

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", writeRecoverFile(t, mnemonic)))
	require.NoError(t, cmd.Flags().Set("logger.level", "bogus"))
	cmd.SetOut(&bytes.Buffer{})

	require.Error(t, runInit(cmd, nil))
	require.False(t, hostkey.Exist(filepath.Join(home, "keys")), "keys must not be recovered when the command is going to fail anyway")

	// fixing the typo and retrying the exact same recovery must still work
	retry := initCmd()
	require.NoError(t, retry.Flags().Set("home", home))
	require.NoError(t, retry.Flags().Set("recover-file", writeRecoverFile(t, mnemonic)))
	retry.SetOut(&bytes.Buffer{})
	require.NoError(t, runInit(retry, nil))
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

	// Derive independently from the same mnemonic and confirm it's the
	// same operator address init just reported.
	want, _, err := hostkey.Ensure(t.TempDir(), mnemonic)
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
// that check up front, hostconfig.Ensure would have already created
// config.toml and every directory by the time hostkey.Ensure got around
// to rejecting the mnemonic.
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

// Recovering only makes sense for a host that doesn't have keys yet.
// This is checked before anything else, so a config.toml never gets
// written on the way to rejecting it, which would otherwise block
// every retry, even a correct plain init, with "config already
// exists", without ever reaching the key check again.
func TestRunInitRejectsRecoverFileOnceKeysExist(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedKeys(t, home) // pre-existing keys, as if config.toml alone was reset by hand

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", writeRecoverFile(t, testMnemonic(t))))
	cmd.SetOut(&bytes.Buffer{})

	err := runInit(cmd, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--recover-file", "the error should point at which flag to drop")
	require.NoFileExists(t, filepath.Join(home, "config.toml"))

	// A plain, correct retry must now succeed.
	retry := initCmd()
	require.NoError(t, retry.Flags().Set("home", home))
	retry.SetOut(&bytes.Buffer{})
	require.NoError(t, runInit(retry, nil))
}

// When init only has to add keys to a config that already exists, it
// must report what's actually saved on disk, not the fresh defaults
// Prepare resolved before ever reading that file.
func TestRunInitSetsUpKeysForExistingConfigReportsRealSavedConfig(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	prepared, err := hostconfig.Prepare(home, "", map[string]any{"logger.level": "warn"})
	require.NoError(t, err)
	_, err = hostconfig.Commit(prepared)
	require.NoError(t, err)

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.Contains(t, out.String(), "Config already exists")
	require.Contains(t, out.String(), "level=warn")
	require.NotContains(t, out.String(), "level=info")
}

// A re-run against an existing config must never apply override
// flags: unlike start, where a per-run override is expected, init
// reports what's actually saved, and showing a value that won't
// persist would be misleading. It must say so plainly instead.
func TestRunInitIgnoresOverridesOnExistingConfig(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedConfig(t, home) // default logger.level, "info"

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("logger.level", "warn"))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.Contains(t, out.String(), "level=info", "the override must not be shown as if it took effect")
	require.NotContains(t, out.String(), "level=warn")
	require.Contains(t, out.String(), "override flags were ignored")

	saved, err := os.ReadFile(filepath.Join(home, "config.toml"))
	require.NoError(t, err)
	require.Contains(t, string(saved), "'info'", "the saved file itself must be untouched")
}

// --data-dir's "not saved" reminder must still show up even when init
// only had to add keys to, or confirm, an already-existing config,
// not just on a fully fresh instance.
func TestRunInitWarnsWhenDataDirSetOnExistingConfig(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedConfig(t, home)
	dataDir := filepath.Join(t.TempDir(), "bigdisk")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("data-dir", dataDir))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runInit(cmd, nil))
	require.Contains(t, out.String(), "not saved")
}
