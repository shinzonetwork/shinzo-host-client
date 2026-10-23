package logger

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/term"
)

// Config configures a logger. It's deliberately independent of
// hostconfig.LoggerConfig (same two core fields, different package) so
// this stays reusable without importing hostconfig; callers translate
// explicitly at the call site.
type Config struct {
	// Level is the minimum level to log: debug, info, warn, or error.
	// Empty defaults to info, or debug if Development is true.
	Level string

	// Development defaults Level to debug instead of info when Level is
	// left empty.
	Development bool

	// OutputPaths is where log entries are written: "stdout", "stderr",
	// or a file path. Defaults to ["stdout"] if empty. Each path gets
	// its own core, Tee'd together, so e.g. a human-readable console on
	// stdout and JSON to a file can run side by side.
	OutputPaths []string

	// Fields are attached to every entry this logger writes.
	Fields map[string]any
}

// New builds a logger writing to every path in cfg.OutputPaths. A real
// terminal gets a human-readable console encoder; anything else
// (a file, a pipe) gets JSON. The returned func flushes and closes every
// output; call it (deferred) before the process exits.
func New(cfg Config) (*zap.Logger, func() error, error) {
	level, err := parseLevel(cfg.Level, cfg.Development)
	if err != nil {
		return nil, nil, err
	}

	outputs := cfg.OutputPaths
	if len(outputs) == 0 {
		outputs = []string{"stdout"}
	}

	var cores []zapcore.Core
	var closers []func()

	for _, path := range outputs {
		writer, closeWriter, err := zap.Open(path)
		if err != nil {
			for _, closeFn := range closers {
				closeFn()
			}
			return nil, nil, fmt.Errorf("opening log output %q: %w", path, err)
		}
		closers = append(closers, closeWriter)

		cores = append(cores, zapcore.NewCore(encoderFor(path), writer, level))
	}

	opts := []zap.Option{zap.AddCaller()}
	if cfg.Development {
		opts = append(opts, zap.Development())
	}

	log := zap.New(zapcore.NewTee(cores...), opts...)
	for k, v := range cfg.Fields {
		log = log.With(zap.Any(k, v))
	}

	sync := func() error {
		var errs []error
		if err := log.Sync(); err != nil && !isBenignSyncError(err) {
			errs = append(errs, fmt.Errorf("syncing logger: %w", err))
		}
		for _, closeFn := range closers {
			closeFn()
		}
		return errors.Join(errs...)
	}

	return log, sync, nil
}

func encoderFor(path string) zapcore.Encoder {
	if isRealTerminal(path) {
		return newConsoleEncoder()
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "ts"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewJSONEncoder(encoderConfig)
}

// IsTerminal reports whether stdout is a real terminal, for callers that
// need to make the same console-vs-JSON decision New makes internally.
func IsTerminal() bool {
	return isRealTerminal("stdout")
}

func isRealTerminal(path string) bool {
	switch path {
	case "stdout":
		return term.IsTerminal(int(os.Stdout.Fd()))
	case "stderr":
		return term.IsTerminal(int(os.Stderr.Fd()))
	default:
		return false
	}
}

func parseLevel(s string, development bool) (zapcore.Level, error) {
	if s == "" {
		if development {
			return zapcore.DebugLevel, nil
		}
		return zapcore.InfoLevel, nil
	}

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("invalid log level %q: %w", s, err)
	}
	return level, nil
}

// isBenignSyncError reports whether err is one of the well-known cases
// where Sync fails on a destination that doesn't support it (a plain
// terminal or pipe), not a real problem.
func isBenignSyncError(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTTY) ||
		errors.Is(err, syscall.EBADF)
}
