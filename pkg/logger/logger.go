// New is a dependency-injected logger constructor, used alongside the
// existing Init/Sugar globals in zap.go. Not replacing those yet, just
// giving new code a way to build a logger without touching the global.
package logger

import (
	"errors"
	"fmt"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Format is the encoding for a non-terminal destination.
type Format string

const (
	FormatJSON Format = "json"
)

// Config configures New. Zero value is fine, defaults to stdout at info level.
type Config struct {
	// Level is a zap level string ("debug", "info", "warn", "error").
	// Empty means info, or debug if Development is set.
	Level string

	Development bool

	// OutputPaths go straight to zap.Open, so "stdout", "stderr", or a real
	// file path all work. Defaults to stdout.
	OutputPaths []string

	// Fields get attached once at construction, e.g. service name/version.
	Fields map[string]any
}

// New builds a logger and a sync func to defer on shutdown.
func New(cfg Config) (*zap.Logger, func() error, error) {
	level := parseLevel(cfg.Level, cfg.Development)

	outputs := cfg.OutputPaths
	if len(outputs) == 0 {
		outputs = []string{"stdout"}
	}

	var cores []zapcore.Core
	var closers []func() error

	for _, path := range outputs {
		writer, closeWriter, err := zap.Open(path)
		if err != nil {
			return nil, nil, fmt.Errorf("opening log output %q: %w", path, err)
		}
		closers = append(closers, func() error {
			closeWriter()
			return nil
		})

		cores = append(cores, zapcore.NewCore(encoderFor(path, cfg), writer, level))
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
			if err := closeFn(); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}

	return log, sync, nil
}

// only color a real terminal, and only in dev. everything else (files,
// non-dev stdout) gets JSON so it stays grep/parse-able.
func encoderFor(path string, cfg Config) zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "ts"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	isTerminal := path == "stdout" || path == "stderr"

	if isTerminal && cfg.Development {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		return zapcore.NewConsoleEncoder(encoderConfig)
	}

	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewJSONEncoder(encoderConfig)
}

func parseLevel(s string, development bool) zapcore.Level {
	if s == "" {
		if development {
			return zapcore.DebugLevel
		}
		return zapcore.InfoLevel
	}

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return zapcore.InfoLevel
	}
	return level
}

// zap.Sync() on stdout can return EINVAL/ENOTTY/EBADF depending on
// platform, doesn't mean the write was lost, just ignore it.
func isBenignSyncError(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTTY) ||
		errors.Is(err, syscall.EBADF)
}
