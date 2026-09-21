package hostconfig

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
)

func TestRenderIncludesName(t *testing.T) {
	out, err := Render(Config{Name: "host1"})
	require.NoError(t, err)

	require.True(t, bytes.Contains(out, []byte(`name = "host1"`)), "rendered config missing name field: %s", out)
}

func TestRenderProducesCorrectTOML(t *testing.T) {
	cfg := Config{Name: "host1"}

	out, err := Render(cfg)
	require.NoError(t, err)

	var newCfg Config
	require.NoError(t, toml.Unmarshal(out, &newCfg), "rendered output is not a correct toml")
	require.Equal(t, cfg.Name, newCfg.Name, "Name=%q does not match unmarshaled name: %q", cfg.Name, newCfg.Name)
}

func TestRenderOutputIsWritable(t *testing.T) {
	cfg := Config{Name: "host1"}

	out, err := Render(cfg)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "config.toml")

	require.NoError(t, os.WriteFile(path, out, 0o600))
	require.FileExists(t, path)
}
