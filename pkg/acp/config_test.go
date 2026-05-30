package acp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfigFromEnv_DisabledByDefault(t *testing.T) {
	t.Setenv(EnvEnabled, "")
	t.Setenv(EnvSourceHubGRPC, "")
	t.Setenv(EnvSourceHubComet, "")
	t.Setenv(EnvPolicyID, "")

	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
}

// Truthy values per strconv.ParseBool ("1", "t", "true", case-insensitive)
// all enable the middleware; anything else is treated as opt-out.
func TestLoadConfigFromEnv_EnabledViaTruthyStrings(t *testing.T) {
	cases := []string{"1", "t", "T", "true", "True", "TRUE"}
	for _, val := range cases {
		t.Run(val, func(t *testing.T) {
			t.Setenv(EnvEnabled, val)
			t.Setenv(EnvSourceHubGRPC, testGRPCAddr)
			t.Setenv(EnvSourceHubComet, testCometAddr)
			t.Setenv(EnvPolicyID, testPolicyID)

			cfg, err := LoadConfigFromEnv()
			require.NoError(t, err)
			require.True(t, cfg.Enabled)
		})
	}
}

func TestLoadConfigFromEnv_UnparseableFlagDisables(t *testing.T) {
	t.Setenv(EnvEnabled, "yesplease")

	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	require.False(t, cfg.Enabled, "unparseable boolean must default to disabled, not error")
}

// When enabled, missing endpoint values must error rather than silently
// produce a half-configured Authorizer. Each missing field is reported
// individually so the operator can see which variable is wrong.
func TestLoadConfigFromEnv_EnabledRequiresAllEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		grpc     string
		comet    string
		policy   string
		expectIn string
	}{
		{name: "missing grpc", comet: testCometAddr, policy: testPolicyID, expectIn: EnvSourceHubGRPC},
		{name: "missing comet", grpc: testGRPCAddr, policy: testPolicyID, expectIn: EnvSourceHubComet},
		{name: "missing policy", grpc: testGRPCAddr, comet: testCometAddr, expectIn: EnvPolicyID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(EnvEnabled, "true")
			t.Setenv(EnvSourceHubGRPC, tt.grpc)
			t.Setenv(EnvSourceHubComet, tt.comet)
			t.Setenv(EnvPolicyID, tt.policy)

			cfg, err := LoadConfigFromEnv()
			require.ErrorIs(t, err, ErrConfigIncomplete)
			require.Contains(t, err.Error(), tt.expectIn)
			require.True(t, cfg.Enabled, "Enabled should still reflect the env var even when validation fails, so the operator can see what was attempted")
		})
	}
}

func TestLoadConfigFromEnv_EnabledWithAllParamsSucceeds(t *testing.T) {
	const realPolicyID = "3542e098142bb863819519bf54661a1df3b6e32d5ca42b4687132c0e5b81a066"
	t.Setenv(EnvEnabled, "true")
	t.Setenv(EnvSourceHubGRPC, testGRPCAddr)
	t.Setenv(EnvSourceHubComet, testCometAddr)
	t.Setenv(EnvPolicyID, realPolicyID)

	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, testGRPCAddr, cfg.SourceHubGRPC)
	require.Equal(t, testCometAddr, cfg.SourceHubComet)
	require.Equal(t, realPolicyID, cfg.PolicyID)
}

// Validate is exposed so callers that build Config programmatically
// (not via env) can run the same check. Verify it agrees with the env
// loader's enforcement.
func TestConfig_Validate_AgreesWithLoader(t *testing.T) {
	disabled := Config{Enabled: false}
	require.NoError(t, disabled.Validate())

	enabledIncomplete := Config{Enabled: true, SourceHubGRPC: testGRPCAddr}
	require.ErrorIs(t, enabledIncomplete.Validate(), ErrConfigIncomplete)

	enabledComplete := Config{
		Enabled:        true,
		SourceHubGRPC:  testGRPCAddr,
		SourceHubComet: testCometAddr,
		PolicyID:       testPolicyID,
	}
	require.NoError(t, enabledComplete.Validate())
}
