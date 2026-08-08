package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
