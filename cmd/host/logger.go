package main

import (
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/logger"
)

// newHostLogger builds the logger for a command from its resolved
// config. The caller must defer flush(&err) immediately. A flush
// failure then surfaces as the command's own error, but only if
// nothing else already failed first.
func newHostLogger(cfg hostconfig.Config) (log *zap.Logger, flush func(*error), err error) {
	log, sync, err := logger.New(logger.Config{
		Level:       cfg.Logger.Level,
		Development: cfg.Logger.Development,
	})
	if err != nil {
		return nil, nil, err
	}

	return log, func(outerErr *error) {
		if syncErr := sync(); syncErr != nil && *outerErr == nil {
			*outerErr = syncErr
		}
	}, nil
}
