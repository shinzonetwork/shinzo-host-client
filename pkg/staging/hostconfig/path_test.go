package hostconfig

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultPathIsCorrect(t *testing.T) {
	path, err := DefaultPath("host1")
	require.NoError(t, err)

	require.Contains(t, path, filepath.Join("host", "host1", "config.toml"), "Default: %q, missing expected suffix", path)
}
