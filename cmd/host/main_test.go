package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDebugMuxServesProfiles(t *testing.T) {
	mux := newDebugMux()

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/debug/pprof/"},
		{http.MethodGet, "/debug/pprof/heap"},
		{http.MethodGet, "/debug/pprof/cmdline"},
		{http.MethodGet, "/debug/pprof/symbol"},
		{http.MethodPost, "/debug/pprof/symbol"},
	}

	for _, c := range cases {
		req := httptest.NewRequest(c.method, c.path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("%s %s: got %d, want %d", c.method, c.path, rec.Code, http.StatusOK)
		}
	}
}

func TestDebugMuxRejectsWritesToReadEndpoints(t *testing.T) {
	mux := newDebugMux()

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/cmdline"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s: got %d, want %d", path, rec.Code, http.StatusMethodNotAllowed)
		}
	}
}

func TestDebugMuxServesStorageCounters(t *testing.T) {
	mux := newDebugMux()

	req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want %d", rec.Code, http.StatusOK)
	}

	for _, name := range []string{"badger_write_bytes_user", "badger_write_bytes_compaction"} {
		if !strings.Contains(rec.Body.String(), name) {
			t.Errorf("%s missing from /debug/vars", name)
		}
	}
}

func TestServeDebugReportsAnUnusableAddress(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a port: %v", err)
	}
	defer func() { _ = held.Close() }()

	if err := serveDebug(held.Addr().String()); err == nil {
		t.Error("serveDebug accepted an address already in use")
	}
}

var canaryOnce sync.Once

func TestServeDebugServesProfilingOnly(t *testing.T) {
	const canary = "/debug/serve-debug-canary"
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

	if err := serveDebug(addr); err != nil {
		t.Fatalf("serveDebug(%s): %v", addr, err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	get := func(path string) int {
		resp, err := client.Get("http://" + addr + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
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
