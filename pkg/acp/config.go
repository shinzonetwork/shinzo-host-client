package acp

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

// ErrConfigIncomplete is returned by Config.Validate when the middleware
// is enabled but a required endpoint value is missing. Callers branch on
// it via errors.Is; the formatted error includes the offending env var
// name for operator debugging.
var ErrConfigIncomplete = errors.New("acp middleware enabled without required endpoint")

// Environment variable names the middleware reads at startup.
const (
	EnvEnabled        = "ACP_MIDDLEWARE_ENABLED"
	EnvSourceHubGRPC  = "SOURCEHUB_GRPC_ADDR"
	EnvSourceHubComet = "SOURCEHUB_COMET_RPC"
	EnvPolicyID       = "SHINZO_POLICY_ID"
)

// Config holds the runtime configuration for the access-control middleware.
//
// Enabled toggles the middleware on or off without removing it from the
// handler chain: when false, requests pass through to defradb unchanged
// and no SourceHub query fires. The three endpoint fields are consumed by
// SourceHubAuthorizer when Enabled is true.
type Config struct {
	Enabled        bool
	SourceHubGRPC  string
	SourceHubComet string
	PolicyID       string
}

// LoadConfigFromEnv reads the middleware configuration from the process
// environment. When Enabled is true, missing endpoint values return an
// error so the host fails to start rather than silently running ungated.
func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		Enabled:        parseBool(os.Getenv(EnvEnabled)),
		SourceHubGRPC:  os.Getenv(EnvSourceHubGRPC),
		SourceHubComet: os.Getenv(EnvSourceHubComet),
		PolicyID:       os.Getenv(EnvPolicyID),
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// Validate returns an error when the middleware is enabled but any of the
// endpoint values is missing. A disabled middleware is always valid; its
// endpoint fields may be empty and the middleware will short-circuit.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.SourceHubGRPC == "" {
		return fmt.Errorf("%w: %s", ErrConfigIncomplete, EnvSourceHubGRPC)
	}
	if c.SourceHubComet == "" {
		return fmt.Errorf("%w: %s", ErrConfigIncomplete, EnvSourceHubComet)
	}
	if c.PolicyID == "" {
		return fmt.Errorf("%w: %s", ErrConfigIncomplete, EnvPolicyID)
	}
	return nil
}

// parseBool returns true only for explicit truthy strings ("1", "t",
// "true", etc. per strconv.ParseBool). An unset variable or unparseable
// value is treated as false so the middleware is opt-in.
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
