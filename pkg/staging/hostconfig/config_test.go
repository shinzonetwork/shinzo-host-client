package hostconfig

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// setupConfig is Prepare+Commit together, for tests that just need a
// provisioned instance and aren't testing either step itself.
func setupConfig(t *testing.T, home, dataDir string, overrides map[string]any) Config {
	t.Helper()
	prepared, err := Prepare(home, dataDir, overrides)
	require.NoError(t, err)
	cfg, err := Commit(prepared)
	require.NoError(t, err)
	return cfg
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

func TestLoadDecodesOntoExistingConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[logger]
development = false
level = "warn"

[http]
addr = ":9090"
`), 0o600))

	cfg := Config{Home: "/resolved/home", ConfigPath: path}
	require.NoError(t, load(&cfg))

	require.Equal(t, "/resolved/home", cfg.Home, "runtime field must survive a load")
	require.Equal(t, "warn", cfg.Logger.Level)
	require.False(t, cfg.Logger.Development)
	require.Equal(t, ":9090", cfg.HTTP.Addr)
}

func TestLoadFailsIfFileMissing(t *testing.T) {
	cfg := Config{ConfigPath: filepath.Join(t.TempDir(), "missing.toml")}
	require.Error(t, load(&cfg))
}

func TestLoadFailsOnMalformedTOML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed.toml")
	require.NoError(t, os.WriteFile(path, []byte(`not = [ valww`), 0o600))

	cfg := Config{ConfigPath: path}
	require.Error(t, load(&cfg))
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte("[logger]\nlevle = \"debug\"\n"), 0o600)) //nolint:misspell // deliberate typo of "level", exercising the unknown-field case

	cfg := Config{ConfigPath: path}
	require.Error(t, load(&cfg))
}

func TestDefaultConfigValues(t *testing.T) {
	cfg := defaultConfig()

	require.False(t, cfg.Logger.Development)
	require.Equal(t, "info", cfg.Logger.Level)
	require.Equal(t, ":8080", cfg.HTTP.Addr)
}

func TestApplyDefaultsPreservesLocationFields(t *testing.T) {
	cfg := Config{Home: "/resolved/home", DataDir: "/resolved/data"}

	require.NoError(t, applyDefaults(&cfg))

	require.Equal(t, "/resolved/home", cfg.Home)
	require.Equal(t, "/resolved/data", cfg.DataDir)
	require.Equal(t, defaultConfig().Logger, cfg.Logger)
	require.Equal(t, defaultConfig().HTTP, cfg.HTTP)
}

func TestSetNestedBuildsNestedMap(t *testing.T) {
	m := map[string]any{}
	setNested(m, []string{"logger", "level"}, "info")

	require.Equal(t, map[string]any{
		"logger": map[string]any{"level": "info"},
	}, m)
}

func TestApplyOverridesIgnoresEmptyMap(t *testing.T) {
	cfg := defaultConfig()
	before := cfg

	require.NoError(t, applyOverrides(&cfg, nil))
	require.Equal(t, before, cfg)
}

func TestApplyOverridesSetsNestedFieldOnly(t *testing.T) {
	cfg := defaultConfig() // Logger.Development=false, Logger.Level="info"

	require.NoError(t, applyOverrides(&cfg, map[string]any{
		"logger.level": "warn",
	}))

	require.Equal(t, "warn", cfg.Logger.Level)
	require.False(t, cfg.Logger.Development, "sibling field not mentioned in the override must survive untouched")
}

func TestApplyOverridesAcrossMultipleSections(t *testing.T) {
	cfg := defaultConfig()

	require.NoError(t, applyOverrides(&cfg, map[string]any{
		"logger.development": true,
		"http.addr":          ":9090",
	}))

	require.True(t, cfg.Logger.Development)
	require.Equal(t, "info", cfg.Logger.Level, "untouched field must survive")
	require.Equal(t, ":9090", cfg.HTTP.Addr)
}

func TestApplyOverridesNeverTouchesLocationFields(t *testing.T) {
	cfg := Config{Home: "/resolved/home", ConfigPath: "/resolved/home/config.toml"}

	require.NoError(t, applyOverrides(&cfg, map[string]any{"logger.level": "warn"}))

	require.Equal(t, "/resolved/home", cfg.Home)
	require.Equal(t, "/resolved/home/config.toml", cfg.ConfigPath)
}

func TestCreateBuildsAndSavesConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	cfg := defaultConfig()
	cfg.ConfigPath = path

	require.NoError(t, create(cfg))
	require.FileExists(t, path)
}

func TestCreateFailsIfAlreadyExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	cfg := defaultConfig()
	cfg.ConfigPath = path

	require.NoError(t, create(cfg))
	require.ErrorIs(t, create(cfg), errAlreadyExists)
}

func TestCreateRejectsInvalidLogLevel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	cfg := defaultConfig()
	cfg.ConfigPath = path
	cfg.Logger.Level = "bogus"

	require.ErrorIs(t, create(cfg), errInvalidLevel)
	require.NoFileExists(t, path)
}

func TestPrepareCreatesDirsButNotConfigYet(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cfg, err := Prepare(home, "", nil)
	require.NoError(t, err)

	require.Equal(t, filepath.Join(home, "config.toml"), cfg.ConfigPath)
	require.NoFileExists(t, cfg.ConfigPath)
	require.DirExists(t, cfg.DataDir)
	require.DirExists(t, cfg.KeyDir)
	require.DirExists(t, cfg.FilterDir)
}

func TestCommitWritesTheConfigPrepareResolved(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	prepared, err := Prepare(home, "", nil)
	require.NoError(t, err)

	cfg, err := Commit(prepared)
	require.NoError(t, err)
	require.FileExists(t, cfg.ConfigPath)
}

func TestCommitFailsIfConfigAlreadyExists(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	first, err := Prepare(home, "", nil)
	require.NoError(t, err)
	_, err = Commit(first)
	require.NoError(t, err)

	second, err := Prepare(home, "", nil)
	require.NoError(t, err)
	_, err = Commit(second)
	require.ErrorIs(t, err, errAlreadyExists)
}

func TestPrepareUsesFixedDefaultHome(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)

	cfg, err := Prepare("", "", nil)
	require.NoError(t, err)
	_, err = Commit(cfg)
	require.NoError(t, err)

	require.Equal(t, filepath.Join(fakeHome, ".shinzo", "host"), cfg.Home)
	require.FileExists(t, filepath.Join(fakeHome, ".shinzo", "host", "config.toml"))
}

func TestPrepareAcceptsCustomDataDir(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	dataDir := filepath.Join(t.TempDir(), "bigdisk")

	cfg, err := Prepare(home, dataDir, nil)
	require.NoError(t, err)

	require.Equal(t, dataDir, cfg.DataDir)
	require.DirExists(t, dataDir)
}

func TestCommitBakesOverridesIntoSavedFile(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	prepared, err := Prepare(home, "", map[string]any{"logger.level": "warn"})
	require.NoError(t, err)
	require.Equal(t, "warn", prepared.Logger.Level)

	cfg, err := Commit(prepared)
	require.NoError(t, err)

	// Re-read the raw file, independent of Load, to confirm the
	// override actually landed on disk, not just in the returned value.
	reloaded := Config{ConfigPath: cfg.ConfigPath}
	require.NoError(t, load(&reloaded))
	require.Equal(t, "warn", reloaded.Logger.Level)
}

func TestLoadFailsIfExplicitConfigMissing(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	configPath := filepath.Join(t.TempDir(), "elsewhere", "config.toml")

	_, err := Load(home, "", configPath, nil)
	require.Error(t, err)

	// Simulates an explicit path on a drive that isn't mounted yet. The
	// failure shouldn't leave a stray directory behind where that mount
	// point would go, home included. Nothing gets created on this path
	// at all.
	require.NoDirExists(t, home)
	require.NoDirExists(t, filepath.Dir(configPath))
}

// Load never bootstraps, even at the default location. Commit is the
// only thing that creates a config.toml. Without this, start ends up
// creating one it then can't guarantee it owns, see the long history in
// cmd/host for what that caused.
func TestLoadFailsIfMissingAtDefaultLocation(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	_, err := Load(home, "", "", nil)
	require.ErrorIs(t, err, ErrConfigNotFound)
	require.NoFileExists(t, filepath.Join(home, "config.toml"))
	require.NoDirExists(t, home)
}

func TestLoadReadsExistingConfig(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	setupCfg := setupConfig(t, home, "", nil)

	loadCfg, err := Load(home, "", "", nil)
	require.NoError(t, err)

	require.Equal(t, setupCfg, loadCfg)
}

func TestLoadOverridesAreNeverPersisted(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	setupConfig(t, home, "", nil) // plain defaults on disk

	cfg, err := Load(home, "", "", map[string]any{"logger.level": "warn"})
	require.NoError(t, err)
	require.Equal(t, "warn", cfg.Logger.Level, "override must apply to the returned Config")

	// The file on disk must be untouched by the override.
	reloaded := Config{ConfigPath: cfg.ConfigPath}
	require.NoError(t, load(&reloaded))
	require.Equal(t, "info", reloaded.Logger.Level, "override must never be saved back to disk")
}

func TestLoadRejectsInvalidStoredLevel(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	configPath := filepath.Join(t.TempDir(), "elsewhere", "config.toml")
	require.NoError(t, os.MkdirAll(filepath.Dir(configPath), 0o700))
	require.NoError(t, os.WriteFile(configPath, []byte(`
[logger]
level = "bogus"
`), 0o600))

	_, err := Load(home, "", configPath, nil)
	require.ErrorIs(t, err, errInvalidLevel)
}

// A valid stored config is still rejected if an override landing on top
// of it makes the combined result invalid. validate must catch that
// too, not just whatever was already on disk before overrides applied.
func TestLoadRejectsInvalidOverride(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	setupConfig(t, home, "", nil)

	_, err := Load(home, "", "", map[string]any{"logger.level": "bogus"})
	require.ErrorIs(t, err, errInvalidLevel)

	_, err = Load(home, "", "", map[string]any{"http.addr": "notanaddr"})
	require.ErrorIs(t, err, errInvalidAddr)
}

// A typo'd override key (e.g. a CLI flag that doesn't match a real
// hostconfig field) must be a loud error, not silently ignored. This is
// applyOverrides' DisallowUnknownFields decoder, exercised through the
// public Load entry point rather than calling applyOverrides directly.
func TestLoadRejectsUnknownOverrideKey(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	_, err := Load(home, "", "", map[string]any{"logger.leve": "warn"}) //nolint:misspell // deliberate typo of "level", exercising the unknown-field case
	require.Error(t, err)
}

func TestLoadDoesNotRememberPreviousCall(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)

	customHome := filepath.Join(t.TempDir(), "customhome")
	customData := filepath.Join(t.TempDir(), "customdata")

	setupConfig(t, customHome, customData, nil)

	_, err := Load(customHome, customData, "", nil)
	require.NoError(t, err)

	// A bare call afterward must resolve to the fixed defaults, independent
	// of everything just used above. It must fail about fakeHome's own
	// (never set up) location, not silently reuse customHome's.
	_, err = Load("", "", "", nil)
	require.ErrorIs(t, err, ErrConfigNotFound)
	require.ErrorContains(t, err, filepath.Join(fakeHome, ".shinzo", "host"))
}

func TestEnsureCreatesFreshInstanceWhenNoneExists(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cfg, existed, err := Ensure(home, "", map[string]any{"logger.level": "warn"})
	require.NoError(t, err)
	require.False(t, existed)
	require.Equal(t, "warn", cfg.Logger.Level)
	require.FileExists(t, filepath.Join(home, "config.toml"))
}

func TestEnsureLoadsExistingInstance(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	setupConfig(t, home, "", nil)

	cfg, existed, err := Ensure(home, "", nil)
	require.NoError(t, err)
	require.True(t, existed)
	require.Equal(t, filepath.Join(home, "config.toml"), cfg.ConfigPath)
}

// Unlike a fresh instance, overrides must never apply to one that
// already exists: start is where a per-run override is expected,
// init's job here is reporting what's actually saved.
func TestEnsureNeverAppliesOverridesWhenAlreadyExists(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	setupConfig(t, home, "", nil) // default logger.level, "info"

	cfg, existed, err := Ensure(home, "", map[string]any{"logger.level": "warn"})
	require.NoError(t, err)
	require.True(t, existed)
	require.Equal(t, "info", cfg.Logger.Level, "an existing config must never be affected by overrides")
}

// Two Ensure calls racing to create config.toml for the same empty
// home at once must not have the loser fail: it falls back to
// loading what the winner wrote, the same way keys.Ensure does.
func TestEnsureConcurrentCallsNeverFail(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	const n = 8
	errs := make([]error, n)
	existedFlags := make([]bool, n)

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			_, existedFlags[i], errs[i] = Ensure(home, "", nil)
		})
	}
	wg.Wait()

	created := 0
	for i, err := range errs {
		require.NoError(t, err, "losing the race to create must fall back to loading, not error")
		if !existedFlags[i] {
			created++
		}
	}
	require.Equal(t, 1, created, "exactly one concurrent call should freshly create")
}

func TestValidateOverridesAcceptsValidOverrides(t *testing.T) {
	require.NoError(t, ValidateOverrides(map[string]any{"logger.level": "warn", "http.addr": ":9090"}))
}

func TestValidateOverridesRejectsBadLevel(t *testing.T) {
	require.Error(t, ValidateOverrides(map[string]any{"logger.level": "bogus"}))
}

func TestValidateOverridesAcceptsNil(t *testing.T) {
	require.NoError(t, ValidateOverrides(nil))
}
