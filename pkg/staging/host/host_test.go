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

// freeAddr finds a free local port by briefly binding to it, so the
// test doesn't hardcode (and risk colliding on) a fixed one. srv
// itself is given a plain, concrete address either way, same as it
// would be in production.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// Start must actually bind srv: a handler registered on its mux
// beforehand must be reachable once Start returns.
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

// Start must actually wire mountStatus in — mountStatus's own
// response shape is tested in mount_status_test.go, this just
// confirms Start doesn't forget to call it.
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
