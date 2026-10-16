package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/keys"
)

func testNodeKeys(t *testing.T) keys.NodeKeys {
	t.Helper()
	nodeKeys, err := keys.EnsureKeys(t.TempDir(), "", zap.NewNop().Sugar())
	require.NoError(t, err)
	return nodeKeys
}

func TestPrintConfigIncludesEveryFieldExceptHome(t *testing.T) {
	cfg := hostconfig.Config{
		Home:       "/resolved/home",
		DataDir:    "/resolved/data",
		ConfigPath: "/resolved/config.toml",
		KeyDir:     "/resolved/keys",
		FilterDir:  "/resolved/filter",
		Logger:     hostconfig.LoggerConfig{Development: true, Level: "warn"},
		HTTP:       hostconfig.HTTPConfig{Addr: ":9090"},
	}

	nodeKeys := testNodeKeys(t)
	wantAddr, err := nodeKeys.ShinzoAddress()
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, printConfig(&out, cfg, nodeKeys))

	for _, want := range []string{
		"/resolved/data",
		"/resolved/config.toml",
		"/resolved/keys",
		"/resolved/filter",
		"development=true",
		"level=warn",
		"addr=:9090",
		wantAddr,
	} {
		require.Contains(t, out.String(), want)
	}

	// Home is deliberately left out: the caller already prints it in its
	// own confirmation line before calling printConfig.
	require.NotContains(t, out.String(), "/resolved/home")
	require.NotContains(t, out.String(), "Home:")
}

func TestPrintConfigFailsIfWriteFails(t *testing.T) {
	require.Error(t, printConfig(failingWriter{}, hostconfig.Config{}, testNodeKeys(t)))
}

// failingWriter always fails, so printConfig's error path is reachable
// without needing a real broken io.Writer (e.g. a closed pipe).
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}
