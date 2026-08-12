package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sourcenetwork/corelog"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

func TestDebugMuxServesProfiles(t *testing.T) {
	mux := newDebugMux()

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/cmdline"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("%s: got %d, want %d", path, rec.Code, http.StatusOK)
		}
	}
}

// The handler is only reachable through this mux, so a listener that serves pprof must
// serve the level endpoint too or it cannot be changed without recreating the container.
func TestDebugMuxServesLogLevel(t *testing.T) {
	mux := newDebugMux()

	req := httptest.NewRequest(http.MethodPost, logger.LevelPath+"?level="+corelog.LevelError, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST %s: got %d, want %d", logger.LevelPath, rec.Code, http.StatusOK)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decoding the level response: %v", err)
	}
	if body["level"] != corelog.LevelError {
		t.Errorf("level: got %q, want %q", body["level"], corelog.LevelError)
	}
	if got := corelog.GetConfig("").Level; got != corelog.LevelError {
		t.Errorf("the change did not reach corelog: got %q", got)
	}
}

// The listener has to answer from its own mux. A nil handler makes net/http fall back to
// http.DefaultServeMux, which carries whatever any linked package registered on it, so the
// profiling port would start serving unrelated endpoints.
func TestServeDebugServesProfilingOnly(t *testing.T) {
	// Stands in for the handlers packages register on the default mux as a side effect of
	// being linked in, without depending on which of them the binary happens to pull in.
	const canary = "/debug/serve-debug-canary"
	http.DefaultServeMux.HandleFunc(canary, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("failed to release the reserved port: %v", err)
	}

	serveDebug(addr)

	client := &http.Client{Timeout: 2 * time.Second}
	get := func(path string) int {
		// serveDebug starts listening in the background, so the first attempts can race it.
		var lastErr error
		for range 50 {
			resp, err := client.Get("http://" + addr + path)
			if err != nil {
				lastErr = err
				time.Sleep(20 * time.Millisecond)
				continue
			}
			defer func() { _ = resp.Body.Close() }()
			return resp.StatusCode
		}
		t.Fatalf("%s: debug listener never came up: %v", path, lastErr)
		return 0
	}

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", logger.LevelPath} {
		if code := get(path); code != http.StatusOK {
			t.Errorf("%s: got %d, want %d", path, code, http.StatusOK)
		}
	}

	if code := get(canary); code != http.StatusNotFound {
		t.Errorf("%s: got %d, want %d: the debug listener is serving the default mux",
			canary, code, http.StatusNotFound)
	}
}
