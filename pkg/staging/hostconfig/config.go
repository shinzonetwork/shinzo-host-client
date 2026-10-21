package hostconfig

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const (
	configFileMode = 0o600
	dirMode        = 0o700

	logLevelDebug = "debug"
	logLevelInfo  = "info"
	logLevelWarn  = "warn"
	logLevelError = "error"
)

// Config is a Shinzo Host instance. Its settings, plus where
// everything resolved to on disk.
type Config struct {
	// Runtime
	// These are configs for unpersisted data that's still needed at runtime.
	// Add a value here by making sure you set `toml:"-"`.

	// Home is the home path. It can be supplied or resolves to the default ~/.shinzo/host/
	Home string `toml:"-"`

	// DataDir is where the host's data is stored, separated from the home path
	// because it's expected to grow and may need to be mounted in a separate dir.
	DataDir string `toml:"-"`

	// ConfigPath is the path to the config.toml file currently in use.
	// This is either the default config path or one supplied by the user.
	ConfigPath string `toml:"-"`

	// KeyDir is where the host's key material is stored. Always a fixed
	// subdirectory of home. Unlike DataDir there's no override for it.
	KeyDir string `toml:"-"`

	// FilterDir is where event filter rules are stored. Always a fixed
	// subdirectory of home. Unlike DataDir there's no override for it.
	FilterDir string `toml:"-"`

	// Persisted
	// These are configs read from the .toml file. Values are loaded and
	// parsed from the file into this struct, ignoring the runtime configs
	// above. Unlike those, these have a toml value instead of `toml:"-"`.

	Logger LoggerConfig `toml:"logger"`
	HTTP   HTTPConfig   `toml:"http"`
}

// LoggerConfig holds the settings for the host's logger.
type LoggerConfig struct {
	Development bool   `comment:"enable development mode logging (human-readable console output instead of JSON)" toml:"development"`
	Level       string `comment:"minimum log level: debug, info, warn, or error"                                  toml:"level"`
}

// HTTPConfig holds the settings for the consolidated HTTP server.
type HTTPConfig struct {
	Addr string `comment:"address the HTTP server listens on, e.g. :8080" toml:"addr"`
}

// Prepare resolves a brand-new instance and creates every directory the
// host needs, but does not write config.toml yet. The config always
// lands at the fixed path under home. Unlike Load there's no way to
// point it at an arbitrary location, since Prepare is building one
// coherent instance rooted at home, not just a lone file.
//
// Call Commit, with the Config this returns, once everything else
// needed to finish setting the instance up (e.g. generating its keys)
// has also succeeded. Nothing about the instance is written until
// then, so there's nothing to undo if one of those other steps fails.
func Prepare(home, dataDir string, overrides map[string]any) (Config, error) {
	cfg, err := Resolve(home, dataDir, "")
	if err != nil {
		return Config{}, err
	}

	if err := createPath(cfg); err != nil {
		return Config{}, err
	}

	if err := applyDefaults(&cfg); err != nil {
		return Config{}, err
	}

	// Layer whatever the caller explicitly asked for on top of those
	// defaults. Unlike Load, these overrides end up in the saved file,
	// since Prepare/Commit together are a deliberate, one-time
	// provisioning step. Validated now, so a bad override is caught
	// before the caller does anything else with the result, rather
	// than only once Commit tries to write it.
	if err := applyOverrides(&cfg, overrides); err != nil {
		return Config{}, err
	}
	if err := validate(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Commit writes config.toml for a Config returned by Prepare. Fails if
// one already exists at that path.
func Commit(cfg Config) (Config, error) {
	if err := create(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Load returns the Config for an already-initialized instance. The
// config.toml at the resolved location must already exist. Use
// Prepare and Commit to create a new instance. Overrides are applied
// only to the returned, in-memory Config, never saved back to disk.
func Load(home, dataDir, configPath string, overrides map[string]any) (Config, error) {
	cfg, err := Resolve(home, dataDir, configPath)
	if err != nil {
		return Config{}, err
	}

	if _, err := os.Stat(cfg.ConfigPath); errors.Is(err, fs.ErrNotExist) {
		return Config{}, fmt.Errorf("%w: %s", ErrConfigNotFound, cfg.ConfigPath)
	}

	if err := applyDefaults(&cfg); err != nil {
		return Config{}, err
	}

	// Loaded on top of the defaults already applied above, so a config
	// that's missing a key (hand-edited, or from an older version)
	// still gets a sane value instead of a zero one.
	if err := load(&cfg); err != nil {
		return Config{}, err
	}

	// Defensive. config.toml exists, but the rest of the directory
	// structure (data/keys/filter) might not, e.g. an older instance, or
	// one of them got deleted by hand.
	if err := createPath(cfg); err != nil {
		return Config{}, err
	}

	if err := applyOverrides(&cfg, overrides); err != nil {
		return Config{}, err
	}

	if err := validate(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		Logger: LoggerConfig{Development: false, Level: logLevelInfo},
		HTTP:   HTTPConfig{Addr: ":8080"},
	}
}

func applyDefaults(cfg *Config) error {
	data, err := toml.Marshal(defaultConfig())
	if err != nil {
		return err
	}

	dec := toml.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	return dec.Decode(cfg)
}

func applyOverrides(cfg *Config, overrides map[string]any) error {
	if len(overrides) == 0 {
		return nil
	}

	nested := map[string]any{}
	for key, value := range overrides {
		setNested(nested, strings.Split(key, "."), value)
	}

	data, err := toml.Marshal(nested)
	if err != nil {
		return err
	}

	dec := toml.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	return dec.Decode(cfg)
}

func setNested(m map[string]any, path []string, value any) {
	if len(path) == 0 {
		return
	}

	if len(path) == 1 {
		m[path[0]] = value
		return
	}

	next, ok := m[path[0]].(map[string]any)
	if !ok {
		next = map[string]any{}
		m[path[0]] = next
	}

	setNested(next, path[1:], value)
}

func load(cfg *Config) error {
	data, err := os.ReadFile(cfg.ConfigPath) //nolint:gosec // path is operator-supplied, not untrusted input.
	if err != nil {
		return err
	}

	dec := toml.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	return dec.Decode(cfg)
}

func create(cfg Config) error {
	if err := validate(cfg); err != nil {
		return err
	}

	data, err := render(cfg)
	if err != nil {
		return err
	}

	return save(cfg.ConfigPath, data)
}

func save(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, configFileMode) //nolint:gosec // path is operator-supplied, not untrusted input.
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return errAlreadyExists
		}
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}

	return f.Close()
}
