package host

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func TestMountMetrics_ServesJSON(t *testing.T) {
	srv, err := hostserver.New(testHostConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}
	fake := &fakeDefraService{}
	mountMetrics(srv, fake)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %q", ct)
	}
}
