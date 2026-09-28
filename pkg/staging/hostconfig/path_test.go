package hostconfig

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultPathIsCorrect(t *testing.T) {
	home, err := os.UserHomeDir()
	require.NoError(t, err)

	path, err := DefaultPath("host1")
	require.NoError(t, err)

	require.Equal(t, filepath.Join(home, ".shinzo", "host", "host1", "config.toml"), path)
}
