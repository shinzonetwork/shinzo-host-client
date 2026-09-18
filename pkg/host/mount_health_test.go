package host

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

func TestMountHealth_ServesStatus(t *testing.T) {
	srv, err := hostserver.New(testHostConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}
	fake := &fakeDefraService{}
	mountHealth(srv, fake)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for a nil DB, got %d", rec.Code)
	}

	var resp server.HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Status != "unhealthy" {
		t.Fatalf("expected status unhealthy, got %q", resp.Status)
	}
	if resp.DefraDBConnected {
		t.Fatal("expected defradb_connected false for a nil DB")
	}
}
