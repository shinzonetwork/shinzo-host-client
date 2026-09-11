package host

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func TestMountSystemStats_ServesJSON(t *testing.T) {
	cfg := testHostConfig()
	cfg.Store.Path = t.TempDir()
	writeSizedFile(t, filepath.Join(cfg.Store.Path, "data.bin"), 2048)

	srv, err := hostserver.New(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}
	mountSystemStats(srv, cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/system", nil)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %q", ct)
	}

	var stats SystemStats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.Disk.ShinzoBytes != 2048 {
		t.Fatalf("expected shinzo_bytes 2048, got %d", stats.Disk.ShinzoBytes)
	}
}

func TestMountSystemStats_TrailingSlash(t *testing.T) {
	cfg := testHostConfig()
	cfg.Store.Path = t.TempDir()

	srv, err := hostserver.New(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}
	mountSystemStats(srv, cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/system/", nil)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for the trailing-slash form, got %d", rec.Code)
	}
}

func TestMountSystemStats_BadPathReportsError(t *testing.T) {
	cfg := testHostConfig()
	cfg.Store.Path = filepath.Join(t.TempDir(), "does-not-exist")

	srv, err := hostserver.New(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}
	mountSystemStats(srv, cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/system", nil)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for a missing store path, got %d", rec.Code)
	}
}
