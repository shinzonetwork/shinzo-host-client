package host

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// Start now blocks until ctx is done, so this runs it in the
// background, confirms a handler is actually reachable while it's
// running, then cancels and checks it returns cleanly.
func TestStartServesWhileRunningThenCleansUpOnCancel(t *testing.T) {
	addr := freeAddr(t)
	srv, err := hostserver.New(addr, zap.NewNop())
	require.NoError(t, err)

	srv.Mux().HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // if an assertion below fails first, this still stops the server
	done := make(chan error, 1)
	go func() { done <- Start(ctx, hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{}, srv) }()

	require.Eventually(t, func() bool { return srv.Addr() != "" }, 2*time.Second, 10*time.Millisecond)

	resp, err := http.Get("http://" + addr + "/ping") //nolint:gosec,noctx // test hits its own just-bound local server
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	require.NoError(t, <-done)
}

// Response shape is tested in mount_status_test.go, this just checks the wiring.
func TestStartMountsStatus(t *testing.T) {
	srv, err := hostserver.New(freeAddr(t), zap.NewNop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, Start(ctx, hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{}, srv))

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
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

	require.Error(t, Start(context.Background(), hostconfig.Config{}, zap.NewNop(), hostkey.NodeKeys{}, srv))
	require.True(t, cleaned, "Start must run registered shutdown fns if binding fails")
}
