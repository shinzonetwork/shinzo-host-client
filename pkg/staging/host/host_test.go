package host

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostserver"
)

// freeAddr grabs a free port by briefly binding to it, so tests don't
// hardcode (and risk colliding on) a fixed one.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func TestStartBindsTheServer(t *testing.T) {
	addr := freeAddr(t)
	srv, err := hostserver.New(addr, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	srv.Mux().HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})

	err = Start(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{}, srv)
	require.NoError(t, err)

	resp, err := http.Get("http://" + addr + "/ping") //nolint:gosec,noctx // test hits its own just-bound local server
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// Response shape is tested in mount_status_test.go, this just checks the wiring.
func TestStartMountsStatus(t *testing.T) {
	addr := freeAddr(t)
	srv, err := hostserver.New(addr, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	require.NoError(t, Start(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{}, srv))

	resp, err := http.Get("http://" + addr + "/status") //nolint:gosec,noctx // test hits its own just-bound local server
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestStartCleansUpIfBindFails(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = blocker.Close() }()

	srv, err := hostserver.New(blocker.Addr().String(), zap.NewNop())
	require.NoError(t, err)

	var cleaned bool
	srv.RegisterShutdown(func(context.Context) error {
		cleaned = true
		return nil
	})

	require.Error(t, Start(hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{}, srv))
	require.True(t, cleaned, "Start must run registered shutdown fns if binding fails")
}
