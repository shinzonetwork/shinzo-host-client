package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
)

func TestNewHostLoggerBuildsFromConfig(t *testing.T) {
	log, flush, err := newHostLogger(hostconfig.Config{Logger: hostconfig.LoggerConfig{Level: "warn"}})
	require.NoError(t, err)
	require.NotNil(t, log)

	var flushErr error
	flush(&flushErr)
	require.NoError(t, flushErr)
}

func TestNewHostLoggerRejectsInvalidLevel(t *testing.T) {
	_, _, err := newHostLogger(hostconfig.Config{Logger: hostconfig.LoggerConfig{Level: "bogus"}})
	require.Error(t, err)
}

// flush must never overwrite an error the caller already has.
func TestNewHostLoggerFlushNeverOverwritesAnExistingError(t *testing.T) {
	_, flush, err := newHostLogger(hostconfig.Config{})
	require.NoError(t, err)

	original := errors.New("the command's own error")
	outerErr := original
	flush(&outerErr)
	require.Equal(t, original, outerErr)
}
