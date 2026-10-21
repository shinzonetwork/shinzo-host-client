package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/keys"
)

// seedConfig pre-creates config.toml, since start no longer bootstraps
// one itself.
func seedConfig(t *testing.T, home string) {
	t.Helper()
	prepared, err := hostconfig.Prepare(home, "", nil)
	require.NoError(t, err)
	_, err = hostconfig.Commit(prepared)
	require.NoError(t, err)
}

// seedKeys pre-creates keys, since start no longer generates them itself.
func seedKeys(t *testing.T, home string) {
	t.Helper()
	_, _, err := keys.EnsureKeys(filepath.Join(home, "keys"), "", true, zap.NewNop().Sugar())
	require.NoError(t, err)
}

// start requires init to have run first. It never creates config.toml
// itself, not even at the default location.
func TestRunStartFailsIfConfigMissing(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	cmd.SetOut(&bytes.Buffer{})

	err := runStart(cmd, nil)
	require.ErrorIs(t, err, hostconfig.ErrConfigNotFound)
	require.NoFileExists(t, filepath.Join(home, "config.toml"))
}

func TestRunStartFailsIfExplicitConfigMissing(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	configPath := filepath.Join(t.TempDir(), "elsewhere", "config.toml")

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("config", configPath))

	require.Error(t, runStart(cmd, nil))
}

func TestRunStartOverridesAreNeverPersisted(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedConfig(t, home)
	seedKeys(t, home)

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("logger.level", "warn"))
	cmd.SetOut(&bytes.Buffer{})
	require.NoError(t, runStart(cmd, nil))

	reloaded, err := hostconfig.Load(home, "", "", nil)
	require.NoError(t, err)
	require.Equal(t, "info", reloaded.Logger.Level, "override must never be saved back to disk")
}

// Bool overrides go through a different branch of getOverrides (GetBool,
// not GetString). Check one reaches the in-memory Config the same way the
// string override above does.
func TestRunStartAppliesBoolOverride(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedConfig(t, home)
	seedKeys(t, home)

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("logger.development", "true"))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runStart(cmd, nil))
	require.Contains(t, out.String(), "development=true")
}

func TestRunStartSucceedsWithExplicitExistingConfig(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	configDir := filepath.Join(t.TempDir(), "elsewhere")
	configPath := filepath.Join(configDir, "config.toml")

	seedConfig(t, home)
	seedKeys(t, home)

	// Move the saved config somewhere unrelated to home, to prove --config
	// can point anywhere, not just at home's own default location.
	require.NoError(t, os.MkdirAll(configDir, 0o700))
	require.NoError(t, os.Rename(filepath.Join(home, "config.toml"), configPath))

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("config", configPath))
	cmd.SetOut(&bytes.Buffer{})

	require.NoError(t, runStart(cmd, nil))
}

func TestRunStartUsesCustomDataDir(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	dataDir := filepath.Join(t.TempDir(), "bigdisk")
	seedConfig(t, home)
	seedKeys(t, home)

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("data-dir", dataDir))
	cmd.SetOut(&bytes.Buffer{})

	require.NoError(t, runStart(cmd, nil))
	require.DirExists(t, dataDir)
}

func TestRunStartRejectsInvalidStoredLevel(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	require.NoError(t, os.MkdirAll(home, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(home, "config.toml"), []byte(`
[logger]
level = "bogus"
`), 0o600))

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	cmd.SetOut(&bytes.Buffer{})

	require.Error(t, runStart(cmd, nil))
}

// The output should reflect what was actually loaded from disk, not just
// whatever the caller overrides.
func TestRunStartPrintsFullResolvedConfig(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	prepared, err := hostconfig.Prepare(home, "", map[string]any{"http.addr": ":9191"})
	require.NoError(t, err)
	_, err = hostconfig.Commit(prepared)
	require.NoError(t, err)
	seedKeys(t, home)

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runStart(cmd, nil))
	require.Contains(t, out.String(), "addr=:9191")
}

// Demonstrates the documented data-dir tradeoff, at the CLI level rather
// than hostconfig's own. --data-dir isn't persisted, so init --data-dir X
// followed by a bare start doesn't remember X, start falls back to the
// default location instead, a different, still-empty directory. Not a
// bug. This is what happens if you forget to repeat the flag.
func TestRunStartWithoutDataDirDoesNotReuseInitsCustomDataDir(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	customDataDir := filepath.Join(t.TempDir(), "bigdisk")

	cmd := initCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("data-dir", customDataDir))
	cmd.SetOut(&bytes.Buffer{})
	require.NoError(t, runInit(cmd, nil))

	cmd = startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	// Deliberately not repeating --data-dir here.

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runStart(cmd, nil))
	require.Contains(t, out.String(), filepath.Join(home, "data"), "start without --data-dir falls back to the default, not init's custom one")
	require.NotContains(t, out.String(), customDataDir)
}

func TestStartHelpMentionsInitIsRequiredFirst(t *testing.T) {
	out, err := execute("start", "--help")
	require.NoError(t, err)
	require.Contains(t, out, "run init first")
}

// An existing instance with no keys and no --recover-file must refuse
// to generate one unattended, and must not touch keys at all.
func TestRunStartRefusesToGenerateWithoutRecoverFile(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedConfig(t, home)

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	cmd.SetOut(&bytes.Buffer{})

	err := runStart(cmd, nil)
	require.ErrorIs(t, err, keys.ErrNoKeys)
	require.NoFileExists(t, filepath.Join(home, "keys", "node.key"))
}

// start can recover keys from a mnemonic on an existing instance that
// doesn't have them yet, without ever generating one on its own.
func TestRunStartRecoversKeysFromRecoverFileWhenMissing(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedConfig(t, home)

	mnemonic := testMnemonic(t)
	recoverFile := filepath.Join(t.TempDir(), "mnemonic.txt")
	require.NoError(t, os.WriteFile(recoverFile, []byte(mnemonic), 0o600))

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", recoverFile))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runStart(cmd, nil))
	require.NotContains(t, out.String(), "Generated a new node seed phrase")

	want, _, err := keys.EnsureKeys(filepath.Join(t.TempDir(), "keys"), mnemonic, true, zap.NewNop().Sugar())
	require.NoError(t, err)
	wantAddr, err := want.ShinzoAddress()
	require.NoError(t, err)
	require.Contains(t, out.String(), wantAddr)
}

func TestRunStartFailsOnRecoverMismatch(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	seedConfig(t, home)
	seedKeys(t, home)

	wrongFile := filepath.Join(t.TempDir(), "wrong.txt")
	require.NoError(t, os.WriteFile(wrongFile, []byte(testMnemonic(t)), 0o600))

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))
	require.NoError(t, cmd.Flags().Set("recover-file", wrongFile))
	cmd.SetOut(&bytes.Buffer{})

	require.ErrorIs(t, runStart(cmd, nil), keys.ErrRecoverMismatch)
}

// init and start must agree on the same keys for the same home.
// Whichever one creates them first, the other loads the exact same ones back.
func TestRunInitAndRunStartAgreeOnKeys(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	init := initCmd()
	require.NoError(t, init.Flags().Set("home", home))
	var initOut bytes.Buffer
	init.SetOut(&initOut)
	require.NoError(t, runInit(init, nil))

	start := startCmd()
	require.NoError(t, start.Flags().Set("home", home))
	var startOut bytes.Buffer
	start.SetOut(&startOut)
	require.NoError(t, runStart(start, nil))

	initAddr := extractOperatorAddress(t, initOut.String())
	startAddr := extractOperatorAddress(t, startOut.String())
	require.Equal(t, initAddr, startAddr)
}

func extractOperatorAddress(t *testing.T, out string) string {
	t.Helper()
	const prefix = "Operator address: "
	i := strings.Index(out, prefix)
	require.GreaterOrEqual(t, i, 0, "no operator address line in output:\n%s", out)
	line := out[i+len(prefix):]
	return strings.SplitN(line, "\n", 2)[0]
}
