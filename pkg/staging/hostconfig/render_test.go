package hostconfig

import (
	"bytes"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
)

func TestRenderIncludesHeader(t *testing.T) {
	out, err := render(defaultConfig())
	require.NoError(t, err)

	require.True(t, bytes.Contains(out, []byte("Shinzo Host Config")), "rendered config missing header: %s", out)
}

func TestRenderProducesCorrectTOML(t *testing.T) {
	cfg := defaultConfig()

	out, err := render(cfg)
	require.NoError(t, err)

	var newCfg Config
	require.NoError(t, toml.Unmarshal(out, &newCfg), "rendered output is not a correct toml")
	require.Equal(t, cfg.Logger, newCfg.Logger)
	require.Equal(t, cfg.HTTP, newCfg.HTTP)
}
