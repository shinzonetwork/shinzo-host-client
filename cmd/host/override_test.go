package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestGetOverridesIgnoresUnsetFlags(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("logger.level", "", "")

	overrides, err := getOverrides(cmd)
	require.NoError(t, err)
	require.Empty(t, overrides)
}

func TestGetOverridesReadsChangedStringFlag(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("logger.level", "", "")
	require.NoError(t, cmd.Flags().Set("logger.level", "warn"))

	overrides, err := getOverrides(cmd)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"logger.level": "warn"}, overrides)
}

func TestGetOverridesReadsChangedBoolFlag(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Bool("logger.development", false, "")
	require.NoError(t, cmd.Flags().Set("logger.development", "true"))

	overrides, err := getOverrides(cmd)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"logger.development": true}, overrides)
}

func TestGetOverridesReadsMultipleChangedFlags(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("logger.level", "", "")
	cmd.Flags().String("http.addr", "", "")
	require.NoError(t, cmd.Flags().Set("logger.level", "warn"))
	require.NoError(t, cmd.Flags().Set("http.addr", ":9090"))

	overrides, err := getOverrides(cmd)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"logger.level": "warn",
		"http.addr":    ":9090",
	}, overrides)
}

func TestGetOverridesIsGenericOverAnyDottedFlag(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("some.new.field", "", "")
	require.NoError(t, cmd.Flags().Set("some.new.field", "value"))

	overrides, err := getOverrides(cmd)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"some.new.field": "value"}, overrides)
}

func TestGetOverridesIgnoresFlagsWithoutADot(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("name", "", "")
	require.NoError(t, cmd.Flags().Set("name", "examplenode"))

	overrides, err := getOverrides(cmd)
	require.NoError(t, err)
	require.Empty(t, overrides, "a flag with no dot in its name isn't an override")
}

// Only bool and string override flags exist today, but nothing stops a
// future flag being registered as, say, an int or a duration. getOverrides
// must report that loudly instead of panicking or silently dropping it.
func TestGetOverridesErrorsOnUnsupportedFlagType(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Int("http.timeout", 0, "")
	require.NoError(t, cmd.Flags().Set("http.timeout", "5"))

	_, err := getOverrides(cmd)
	require.ErrorIs(t, err, errUnsupportedOverrideFlagType)
}

func TestInitRegistersOverrideFlags(t *testing.T) {
	cmd := initCmd()

	for _, name := range []string{"logger.development", "logger.level", "http.addr"} {
		require.NotNil(t, cmd.Flags().Lookup(name), "init missing %s", name)
	}
}

func TestStartRegistersOverrideFlags(t *testing.T) {
	cmd := startCmd()

	for _, name := range []string{"logger.development", "logger.level", "http.addr"} {
		require.NotNil(t, cmd.Flags().Lookup(name), "start missing %s", name)
	}
}

func TestInitRegistersHomeAndDataDirButNotConfig(t *testing.T) {
	cmd := initCmd()

	require.NotNil(t, cmd.Flags().Lookup("home"))
	require.NotNil(t, cmd.Flags().Lookup("data-dir"))
	require.Nil(t, cmd.Flags().Lookup("config"), "init builds one instance rooted at home, config path isn't independently settable")
}

func TestStartRegistersHomeDataDirAndConfig(t *testing.T) {
	cmd := startCmd()

	require.NotNil(t, cmd.Flags().Lookup("home"))
	require.NotNil(t, cmd.Flags().Lookup("data-dir"))
	require.NotNil(t, cmd.Flags().Lookup("config"))
}

// --home and --data-dir aren't dotted, so getOverrides must never pick
// them up as config overrides even though they're real, changed flags
// sitting right next to the ones that are.
func TestGetOverridesIgnoresHomeAndDataDirOnRealCommands(t *testing.T) {
	cmd := startCmd()
	require.NoError(t, cmd.Flags().Set("home", "/custom/home"))
	require.NoError(t, cmd.Flags().Set("data-dir", "/custom/data"))
	require.NoError(t, cmd.Flags().Set("config", "/custom/config.toml"))
	require.NoError(t, cmd.Flags().Set("logger.level", "warn"))

	overrides, err := getOverrides(cmd)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"logger.level": "warn"}, overrides)
}
