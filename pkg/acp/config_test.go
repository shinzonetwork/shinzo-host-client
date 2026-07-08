package acp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setBillingEnv sets the middleware env vars for one test, isolating it from
// values inherited from the process environment. The optional ASBaseURL and
// AttesterWindow are cleared here so a test only sees them when it sets them.
func setBillingEnv(t *testing.T, enabled, chainID, minBalance, epochLength string) {
	t.Helper()
	t.Setenv(EnvEnabled, enabled)
	t.Setenv(EnvChainID, chainID)
	t.Setenv(EnvMinQueryBalance, minBalance)
	t.Setenv(EnvEpochLength, epochLength)
	t.Setenv(EnvASBaseURL, "")
	t.Setenv(EnvAttesterWindow, "")
}

func TestLoadConfigFromEnv_DisabledByDefault(t *testing.T) {
	setBillingEnv(t, "", "", "", "")
	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
}

func TestLoadConfigFromEnv_UnparseableFlagDisables(t *testing.T) {
	setBillingEnv(t, "yesplease", "", "", "")
	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	require.False(t, cfg.Enabled, "an unparseable boolean must default to disabled, not error")
}

func TestLoadConfigFromEnv_EnabledRequiresChainID(t *testing.T) {
	setBillingEnv(t, "true", "", "100", "1000")
	_, err := LoadConfigFromEnv()
	require.ErrorIs(t, err, ErrConfigIncomplete)
	require.Contains(t, err.Error(), EnvChainID)
}

func TestLoadConfigFromEnv_EnabledRequiresEpochLength(t *testing.T) {
	setBillingEnv(t, "true", "91273002", "100", "")
	_, err := LoadConfigFromEnv()
	require.ErrorIs(t, err, ErrConfigIncomplete)
	require.Contains(t, err.Error(), EnvEpochLength)
}

func TestLoadConfigFromEnv_RejectsNonNumericChainID(t *testing.T) {
	setBillingEnv(t, "true", "abc", "100", "1000")
	_, err := LoadConfigFromEnv()
	require.ErrorIs(t, err, ErrConfigIncomplete)
}

func TestLoadConfigFromEnv_RejectsNonIntegerBalance(t *testing.T) {
	setBillingEnv(t, "true", "91273002", "1.5", "1000")
	_, err := LoadConfigFromEnv()
	require.ErrorIs(t, err, ErrConfigIncomplete)
	require.Contains(t, err.Error(), EnvMinQueryBalance)
}

func TestLoadConfigFromEnv_EnabledValid(t *testing.T) {
	setBillingEnv(t, "true", "91273002", "1000000", "1000")
	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, uint64(91273002), cfg.ChainID)
	require.Equal(t, uint64(1000), cfg.EpochLength)
	require.Equal(t, "1000000", cfg.MinBalance().String())
}

func TestLoadConfigFromEnv_ParsesAttesterWindow(t *testing.T) {
	setBillingEnv(t, "true", "91273002", "1000000", "1000")
	t.Setenv(EnvAttesterWindow, "30s")
	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, cfg.AttesterWindow)
}

func TestLoadConfigFromEnv_UnsetAttesterWindowIsZero(t *testing.T) {
	setBillingEnv(t, "true", "91273002", "1000000", "1000")
	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	require.Zero(t, cfg.AttesterWindow, "an unset window stays zero so the host applies its default")
}

func TestLoadConfigFromEnv_RejectsMalformedAttesterWindow(t *testing.T) {
	setBillingEnv(t, "true", "91273002", "1000000", "1000")
	t.Setenv(EnvAttesterWindow, "nonsense")
	_, err := LoadConfigFromEnv()
	require.ErrorIs(t, err, ErrConfigIncomplete)
	require.Contains(t, err.Error(), EnvAttesterWindow)
}

func TestConfig_Validate_DisabledAlwaysValid(t *testing.T) {
	require.NoError(t, Config{Enabled: false}.Validate())
}
