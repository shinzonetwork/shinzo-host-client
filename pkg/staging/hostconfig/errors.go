package hostconfig

import "errors"

var (
	errAlreadyExists = errors.New("config already exists")
	errInvalidLevel  = errors.New("invalid log level")
	errInvalidAddr   = errors.New("invalid http address")

	// ErrConfigNotFound is returned by Load when no config.toml exists
	// at the resolved location. Exported, unlike the other sentinels
	// here, so a caller (e.g. start) can give it flag-specific guidance
	// via errors.Is, the same way pkg/staging/hostkey.ErrNoKeys does.
	ErrConfigNotFound = errors.New("config not found")
)
