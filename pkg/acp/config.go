package acp

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"strconv"
)

// ErrConfigIncomplete is returned by Config.Validate when the middleware is
// enabled but a required value is missing or malformed. Callers branch on it via
// errors.Is; the formatted error names the offending env var for debugging.
var ErrConfigIncomplete = errors.New("acp middleware enabled without required config")

// Environment variable names the middleware reads at startup.
const (
	EnvEnabled         = "ACP_MIDDLEWARE_ENABLED"
	EnvChainID         = "SHINZO_CHAIN_ID"
	EnvMinQueryBalance = "SHINZO_MIN_QUERY_BALANCE"
	EnvEpochLength     = "SHINZO_EPOCH_LENGTH"
	EnvASBaseURL       = "SHINZO_AS_BASE_URL"
)

// Config holds the runtime configuration for the query-billing middleware.
//
// Enabled toggles the middleware on or off without removing it from the handler
// chain: when false, requests pass through to defradb unchanged. ChainID is the
// EVM chain id the EIP-712 request signatures are bound to. MinQueryBalance is
// the minimum x/querybalance a payer must hold to be served, as a base-10
// integer string. EpochLength is the number of blocks per settlement epoch, used
// to invalidate the cached balance once per epoch; it must match the accounting
// service's value. ASBaseURL is the accounting service base URL; when empty, the
// gate serves without recording.
type Config struct {
	Enabled         bool
	ChainID         uint64
	MinQueryBalance string
	EpochLength     uint64
	ASBaseURL       string
}

// LoadConfigFromEnv reads the middleware configuration from the process
// environment. When Enabled is true, a missing or malformed value returns an
// error so the host fails to start rather than running mis-gated.
func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		Enabled:         parseBool(os.Getenv(EnvEnabled)),
		MinQueryBalance: os.Getenv(EnvMinQueryBalance),
		ASBaseURL:       os.Getenv(EnvASBaseURL),
	}
	id, err := parseUintEnv(EnvChainID)
	if err != nil {
		return cfg, err
	}
	cfg.ChainID = id
	length, err := parseUintEnv(EnvEpochLength)
	if err != nil {
		return cfg, err
	}
	cfg.EpochLength = length

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// Validate returns an error when the middleware is enabled but a required value
// is unset or malformed. A disabled middleware is always valid.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ChainID == 0 {
		return fmt.Errorf("%w: %s is required", ErrConfigIncomplete, EnvChainID)
	}
	if c.EpochLength == 0 {
		return fmt.Errorf("%w: %s is required", ErrConfigIncomplete, EnvEpochLength)
	}
	if _, ok := new(big.Int).SetString(c.MinQueryBalance, 10); !ok {
		return fmt.Errorf("%w: %s must be a base-10 integer, got %q", ErrConfigIncomplete, EnvMinQueryBalance, c.MinQueryBalance)
	}
	return nil
}

// MinBalance returns MinQueryBalance as a big.Int. It assumes Validate has
// already accepted the value.
func (c Config) MinBalance() *big.Int {
	n, _ := new(big.Int).SetString(c.MinQueryBalance, 10)
	return n
}

// parseUintEnv reads name as an unsigned base-10 integer. An unset variable is
// zero with no error; a malformed value is an error.
func parseUintEnv(name string) (uint64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s is not a valid unsigned integer: %v", ErrConfigIncomplete, name, err)
	}
	return v, nil
}

// parseBool returns true only for explicit truthy strings ("1", "t", "true",
// etc. per strconv.ParseBool). An unset or unparseable value is false, so the
// middleware is opt-in.
func parseBool(s string) bool {
	if s == "" {
		return false
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false
	}
	return b
}
