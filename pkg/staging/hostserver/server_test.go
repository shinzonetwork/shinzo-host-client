package hostserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testLogger(t *testing.T) *zap.Logger {
	t.Helper()
	return zap.NewNop()
}

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

func newTestServer(t *testing.T) (srv *Server, addr string) {
	t.Helper()
	addr = freeAddr(t)
	srv, err := New(addr, testLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	return srv, addr
}

func TestNewRejectsNilLogger(t *testing.T) {
	_, err := New(":0", nil)
	require.ErrorIs(t, err, ErrNilLogger)
}

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

	require.NoError(t, srv.Close())
	require.Equal(t, []int{2, 1}, order, "shutdown funcs must run in reverse of registration order")
}

func TestCloseJoinsErrorsFromShutdownFuncs(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Start())

	boom := errors.New("boom")
	srv.RegisterShutdown(func(context.Context) error { return boom })

	require.ErrorIs(t, srv.Close(), boom)
}

func TestCloseWithoutStartIsSafe(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Close())
}

func TestCloseIsIdempotent(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Start())

	var calls int
	srv.RegisterShutdown(func(context.Context) error {
		calls++
		return nil
	})

	first := srv.Close()
	second := srv.Close()
	require.NoError(t, first)
	require.NoError(t, second)
	require.Equal(t, 1, calls)
}

func TestCloseReplaysFirstCallsError(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Start())

	boom := errors.New("boom")
	srv.RegisterShutdown(func(context.Context) error { return boom })

	require.ErrorIs(t, srv.Close(), boom)
	require.ErrorIs(t, srv.Close(), boom)
}

func TestCloseGivesShutdownFnsTheirOwnBudget(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Start())

	var deadline time.Time
	var hasDeadline bool
	srv.RegisterShutdown(func(ctx context.Context) error {
		deadline, hasDeadline = ctx.Deadline()
		return nil
	})

	before := time.Now()
	require.NoError(t, srv.Close())

	require.True(t, hasDeadline)
	require.InDelta(t, shutdownFnsTimeout.Seconds(), deadline.Sub(before).Seconds(), 1)
}

func TestOnFailureAndCloseOnRealServeFailure(t *testing.T) {
	srv, err := New(":0", testLogger(t))
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = srv.Close() })

	done := make(chan struct{})
	srv.OnFailure(func() { close(done) })

	require.NoError(t, srv.startOn(ln))
	require.NoError(t, ln.Close()) // Serve now fails for a real reason, not via Close/Shutdown

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnFailure never ran after the listener died")
	}

	require.Error(t, srv.Close())
}

func TestStartFailsIfAddressAlreadyBound(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = blocker.Close() }()

	srv, err := New(blocker.Addr().String(), testLogger(t))
	require.NoError(t, err)

	require.Error(t, srv.Start())
}

func TestStartTwiceFails(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Start())
	require.ErrorIs(t, srv.Start(), ErrAlreadyStarted)
}

func TestStartIsRetriableAfterBindFailure(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv, err := New(blocker.Addr().String(), testLogger(t))
	require.NoError(t, err)

	require.Error(t, srv.Start())
	require.NoError(t, blocker.Close())
	require.NoError(t, srv.Start())
	t.Cleanup(func() { _ = srv.Close() })
}

func TestRegisterShutdownDuringCloseIsRaceFree(t *testing.T) {
	srv, _ := newTestServer(t)
	require.NoError(t, srv.Start())

	registering := make(chan struct{})
	go func() {
		defer close(registering)
		for range 100 {
			srv.RegisterShutdown(func(context.Context) error { return nil })
		}
	}()

	_ = srv.Close()
	<-registering
}
