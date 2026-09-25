package hostconfig

import (
	"bytes"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
)

func TestRenderIncludesName(t *testing.T) {
	out, err := render(Config{Name: "host1"})
	require.NoError(t, err)

	require.True(t, bytes.Contains(out, []byte(`name = "host1"`)), "rendered config missing name field: %s", out)
}

func TestRenderProducesCorrectTOML(t *testing.T) {
	cfg := Config{Name: "host1"}

	out, err := render(cfg)
	require.NoError(t, err)

	var newCfg Config
	require.NoError(t, toml.Unmarshal(out, &newCfg), "rendered output is not a correct toml")
	require.Equal(t, cfg.Name, newCfg.Name, "Name=%q does not match unmarshaled name: %q", cfg.Name, newCfg.Name)
}
