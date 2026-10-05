package hostconfig

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestSetupCreatesConfigAndDirs(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cfg, err := Setup(home, "", nil)
	require.NoError(t, err)

	require.Equal(t, filepath.Join(home, "config.toml"), cfg.ConfigPath)
	require.FileExists(t, cfg.ConfigPath)
	require.DirExists(t, cfg.DataDir)
	require.DirExists(t, cfg.KeyDir)
	require.DirExists(t, cfg.FilterDir)
}

func TestSetupFailsIfConfigAlreadyExists(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	_, err := Setup(home, "", nil)
	require.NoError(t, err)

	_, err = Setup(home, "", nil)
	require.ErrorIs(t, err, errAlreadyExists)
}

func TestSetupUsesFixedDefaultHome(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)

	cfg, err := Setup("", "", nil)
	require.NoError(t, err)

	require.Equal(t, filepath.Join(fakeHome, ".shinzo", "host"), cfg.Home)
	require.FileExists(t, filepath.Join(fakeHome, ".shinzo", "host", "config.toml"))
}

func TestSetupAcceptsCustomDataDir(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")
	dataDir := filepath.Join(t.TempDir(), "bigdisk")

	cfg, err := Setup(home, dataDir, nil)
	require.NoError(t, err)

	require.Equal(t, dataDir, cfg.DataDir)
	require.DirExists(t, dataDir)
}

func TestSetupBakesOverridesIntoSavedFile(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cfg, err := Setup(home, "", map[string]any{"logger.level": "warn"})
	require.NoError(t, err)
	require.Equal(t, "warn", cfg.Logger.Level)

	// Re-read the raw file, independent of Setup/Load, to confirm the
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

	// Simulates an explicit path on a drive that isn't mounted yet: the
	// failure shouldn't leave a stray directory behind where that mount
	// point would go, home included — nothing gets created on this path
	// at all.
	require.NoDirExists(t, home)
	require.NoDirExists(t, filepath.Dir(configPath))
}

func TestLoadBootstrapsDefaultLocationIfMissing(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	cfg, err := Load(home, "", "", nil)
	require.NoError(t, err)

	require.FileExists(t, cfg.ConfigPath)
	require.Equal(t, defaultConfig().Logger, cfg.Logger)
}

func TestLoadReadsExistingConfig(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	setupCfg, err := Setup(home, "", nil)
	require.NoError(t, err)

	loadCfg, err := Load(home, "", "", nil)
	require.NoError(t, err)

	require.Equal(t, setupCfg, loadCfg)
}

func TestLoadOverridesAreNeverPersisted(t *testing.T) {
	home := filepath.Join(t.TempDir(), "myhost")

	_, err := Setup(home, "", nil) // plain defaults on disk
	require.NoError(t, err)

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

// A config bootstrapped fresh from defaults is valid on its own, but an
// override landing on top of it can still make it invalid — validate must
// catch that too, not just whatever was in defaults/on disk before
// overrides were applied. Each case gets its own home: the first Load
// call still bootstraps a (valid, default) file on disk before the
// override is applied and rejected, so reusing a home across cases would
// mean the second case silently loads the first one's leftover file
// instead of bootstrapping its own.
func TestLoadRejectsInvalidOverride(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "myhost"), "", "", map[string]any{"logger.level": "bogus"})
	require.ErrorIs(t, err, errInvalidLevel)

	_, err = Load(filepath.Join(t.TempDir(), "myhost"), "", "", map[string]any{"http.addr": "notanaddr"})
	require.ErrorIs(t, err, errInvalidAddr)
}

func TestLoadDoesNotRememberPreviousCall(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)

	customHome := filepath.Join(t.TempDir(), "customhome")
	customData := filepath.Join(t.TempDir(), "customdata")

	_, err := Setup(customHome, customData, nil)
	require.NoError(t, err)

	_, err = Load(customHome, customData, "", nil)
	require.NoError(t, err)

	// A bare call afterward must resolve to the fixed defaults, independent
	// of everything just used above.
	cfg, err := Load("", "", "", nil)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(fakeHome, ".shinzo", "host"), cfg.Home)
}
