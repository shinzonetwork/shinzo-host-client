package hostconfig

import (
	"bytes"
	"errors"
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

// Config is a Shinzo Host instance: its settings, plus where everything
// resolved to on disk.
type Config struct {
	// Runtime
	// These are configs for unpersisted data that's still needed at runtime.
	// Add a value here by making sure you set `toml:"-"`.

	// Home is the home path; it can be supplied or resolves to the default ~/.shinzo/host/
	Home string `toml:"-"`

	// DataDir is where the host's data is stored, separated from the home path
	// because it's expected to grow and may need to be mounted in a separate dir.
	DataDir string `toml:"-"`

	// ConfigPath is the path to the config.toml file currently in use.
	// This is either the default config path or one supplied by the user.
	ConfigPath string `toml:"-"`

	// KeyDir is where the host's key material is stored. Always a fixed
	// subdirectory of home; unlike DataDir there's no override for it.
	KeyDir string `toml:"-"`

	// FilterDir is where event filter rules are stored. Always a fixed
	// subdirectory of home; unlike DataDir there's no override for it.
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

// Setup creates a brand-new instance: resolves home and dataDir, creates
// every directory the host needs, and writes a fresh config.toml built
// from defaults with overrides applied on top. The config always lands at
// the fixed path under home; unlike Load there's no way to point it at an
// arbitrary location, since Setup is building one coherent instance
// rooted at home, not just writing a lone file. Fails if a config already
// exists at that path. Unlike Load, overrides here are baked into the
// saved file, since Setup is always a deliberate, one-time provisioning
// step.
func Setup(home, dataDir string, overrides map[string]any) (Config, error) {
	cfg, err := resolve(home, dataDir, "")
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
	// since Setup is a deliberate, one-time provisioning step.
	if err := applyOverrides(&cfg, overrides); err != nil {
		return Config{}, err
	}

	// Write it. Fails if a config already exists at cfg.ConfigPath.
	if err := create(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Load returns the Config for an instance. If a config already exists at
// the resolved location it's loaded from disk; if not, and configPath was
// left empty (the default location, not an explicit path), a fresh one is
// created from defaults instead. An explicit configPath that doesn't
// exist is always an error, never auto-created. Unlike Setup, overrides
// are applied only to the returned, in-memory Config, never saved to
// disk, whether the file was just created or already existed.
func Load(home, dataDir, configPath string, overrides map[string]any) (Config, error) {
	cfg, err := resolve(home, dataDir, configPath)
	if err != nil {
		return Config{}, err
	}

	_, statErr := os.Stat(cfg.ConfigPath)
	missing := errors.Is(statErr, fs.ErrNotExist)

	// Stat failed for a reason that has nothing to do with the file being
	// missing, e.g. a permissions error, or a parent path that exists but
	// isn't a directory. Always a real problem, surface it as-is.
	if statErr != nil && !missing {
		return Config{}, statErr
	}

	// Explicit path, but nothing's there. This is almost always a mistake,
	// not an intent to create something new: a typo'd path, or pointing
	// at a backup/restore location on a drive that isn't mounted yet.
	// Auto-creating here would silently start a brand-new, empty instance
	// instead of the real one the operator meant to use, and they might
	// not notice for a long time. Error loudly instead.
	if missing && configPath != "" {
		return Config{}, statErr
	}

	if err := applyDefaults(&cfg); err != nil {
		return Config{}, err
	}

	// Only reached once we know we're actually bootstrapping or loading,
	// never on a path we're about to reject above, so an explicit but
	// missing/unmounted path (e.g. a backup drive that isn't mounted yet)
	// is never left with a stray, empty directory created in its place.
	if err := createPath(cfg); err != nil {
		return Config{}, err
	}

	// Default location, nothing there yet. This is just a genuine first
	// run (e.g. starting on a brand-new machine, nothing ever set up).
	// There's no ambiguity about where this should live, so it's safe to
	// create it fresh, entirely from defaults.
	if missing {
		// Check what the final, overridden result would look like before
		// create writes anything to disk: a copy with overrides applied,
		// so a bad override (e.g. an invalid --logger.level) is caught
		// before the file is written, not after. Without this, a failed
		// first run still leaves a (valid, override-less) config.toml
		// behind, since overrides are only applied below, after create.
		candidate := cfg
		if err := applyOverrides(&candidate, overrides); err != nil {
			return Config{}, err
		}
		if err := validate(candidate); err != nil {
			return Config{}, err
		}

		if err := create(cfg); err != nil {
			return Config{}, err
		}
	} else {
		// Either way, and something's actually there: the normal case.
		// Load whatever's saved, on top of the defaults already applied
		// above, so a config that's missing a key (hand-edited, or from
		// an older version) still gets a sane value instead of a zero
		// one.
		if err := load(&cfg); err != nil {
			return Config{}, err
		}

		if err := validate(cfg); err != nil {
			return Config{}, err
		}
	}

	// Applied last, onto the in-memory result only — never saved back,
	// whether cfg was just created above or loaded from an existing file.
	if err := applyOverrides(&cfg, overrides); err != nil {
		return Config{}, err
	}

	// Validated again here, but only for the else branch above: the
	// candidate check in the missing branch already validated this exact
	// combination (defaults + overrides) before create ran, so checking
	// again would just repeat it for free, and validate will only get
	// more expensive to repeat as hostconfig.Config grows. The else
	// branch never went through that check, so it still needs this:
	// without it, a bad override (e.g. an invalid --logger.level) applied
	// on top of an existing, valid file would reach the caller untouched.
	if !missing {
		if err := validate(cfg); err != nil {
			return Config{}, err
		}
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
