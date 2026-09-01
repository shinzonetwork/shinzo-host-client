package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
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

// The listener has to answer from its own mux. A nil handler makes net/http fall back to
// http.DefaultServeMux, which carries whatever any linked package registered on it, so the
// profiling port would start serving unrelated endpoints.
var canaryOnce sync.Once

func TestServeDebugServesProfilingOnly(t *testing.T) {
	// Stands in for the handlers packages register on the default mux as a side effect of
	// being linked in, without depending on which of them the binary happens to pull in.
	const canary = "/debug/serve-debug-canary"
	// The default mux is process-wide and keeps its patterns between runs, so registering
	// per run panics on a repeat pattern.
	canaryOnce.Do(func() {
		http.DefaultServeMux.HandleFunc(canary, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
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

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline"} {
		if code := get(path); code != http.StatusOK {
			t.Errorf("%s: got %d, want %d", path, code, http.StatusOK)
		}
	}

	if code := get(canary); code != http.StatusNotFound {
		t.Errorf("%s: got %d, want %d: the debug listener is serving the default mux",
			canary, code, http.StatusNotFound)
	}
}
