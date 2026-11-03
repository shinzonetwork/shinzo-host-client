package hostserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testLogger(t *testing.T) *zap.Logger {
	t.Helper()
	return zap.NewNop()
}

// freeAddr finds a free local port by briefly binding to it, the
// standard way to get an address safe to hand to a real server in a
// test without hardcoding (and risking colliding on) a fixed port.
// Server itself never needs to know this happened; it's handed a
// plain, concrete address, same as it would be given in production.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func newTestServer(t *testing.T) (srv *Server, addr string) {
	t.Helper()
	addr = freeAddr(t)
	srv, err := New(addr, testLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	return srv, addr
}

func TestNewRejectsNilLogger(t *testing.T) {
	_, err := New(":0", nil)
	require.ErrorIs(t, err, ErrNilLogger)
}

// A handler registered on Mux() before Start must actually be
// reachable, over a real connection, once the server is listening.
func TestStartServesRegisteredHandlers(t *testing.T) {
	srv, addr := newTestServer(t)
	srv.Mux().HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})

	require.NoError(t, srv.Start())

	resp, err := http.Get("http://" + addr + "/ping") //nolint:gosec,noctx // test hits its own just-bound local server
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "pong", string(body))
}

func TestCloseRunsShutdownFuncsInReverseOrder(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Start())

	var order []int
	srv.RegisterShutdown(func(context.Context) error {
		order = append(order, 1)
		return nil
	})
	srv.RegisterShutdown(func(context.Context) error {
		order = append(order, 2)
		return nil
	})

	require.NoError(t, srv.Close(context.Background()))
	require.Equal(t, []int{2, 1}, order, "shutdown funcs must run in reverse of registration order")
}

func TestCloseJoinsErrorsFromShutdownFuncs(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Start())

	boom := errors.New("boom")
	srv.RegisterShutdown(func(context.Context) error { return boom })

	require.ErrorIs(t, srv.Close(context.Background()), boom)
}

// Close before Start must not panic: httpSrv is still nil at that point.
func TestCloseWithoutStartIsSafe(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Close(context.Background()))
}

func TestStartFailsIfAddressAlreadyBound(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = blocker.Close() }()

	srv, err := New(blocker.Addr().String(), testLogger(t))
	require.NoError(t, err)

	require.Error(t, srv.Start())
}
