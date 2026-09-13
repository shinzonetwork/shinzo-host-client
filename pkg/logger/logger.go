package logger

import (
	"errors"
	"fmt"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Format string

const (
	FormatJSON Format = "json"
)

type Config struct {
	Level string

	Development bool

	OutputPaths []string

	Fields map[string]any
}

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

func isBenignSyncError(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTTY) ||
		errors.Is(err, syscall.EBADF)
}
