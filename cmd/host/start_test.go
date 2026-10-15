package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
)

func TestRunStartBootstrapsIfMissing(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runStart(cmd, nil))
	require.FileExists(t, filepath.Join(home, "config.toml"))
	require.Contains(t, out.String(), home)
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

	_, err := hostconfig.Setup(home, "", nil) // plain defaults on disk
	require.NoError(t, err)

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
// not GetString); check one reaches the in-memory Config the same way the
// string override above does.
func TestRunStartAppliesBoolOverride(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

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

	_, err := hostconfig.Setup(home, "", nil)
	require.NoError(t, err)

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

	_, err := hostconfig.Setup(home, "", map[string]any{"http.addr": ":9191"})
	require.NoError(t, err)

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runStart(cmd, nil))
	require.Contains(t, out.String(), "addr=:9191")
}

// Demonstrates the documented data-dir tradeoff, at the CLI level rather
// than hostconfig's own: --data-dir isn't persisted, so init --data-dir X
// followed by a bare start doesn't remember X — start falls back to the
// default location instead, a different, still-empty directory. Not a
// bug; this is what happens if you forget to repeat the flag.
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

// start bootstraps a config when none exists, which makes init optional
// for the default case. The help text needs to say so, so operators
// aren't surprised either way: that it works without init, or that init
// does something start alone doesn't (saving overrides to disk). Checking
// for just the keyword rather than the exact phrase, so a rewording of
// the Long description doesn't break this test over nothing.
func TestStartHelpMentionsInitIsOptional(t *testing.T) {
	out, err := execute("start", "--help")
	require.NoError(t, err)
	require.Contains(t, out, "optional")
}

func TestRunStartGeneratesKeysOnFirstRun(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", home))

	var out bytes.Buffer
	cmd.SetOut(&out)

	require.NoError(t, runStart(cmd, nil))
	require.FileExists(t, filepath.Join(home, "keys", "operator.key"))
	require.Contains(t, out.String(), "Operator address: shinzo1")
}

// init and start must agree on the same keys for the same home: whichever
// one creates them first, the other loads the exact same ones back.
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
