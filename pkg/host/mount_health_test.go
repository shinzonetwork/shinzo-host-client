package host

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMountHealth_ServesOK(t *testing.T) {
	mux := http.NewServeMux()
	mountHealth(mux)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}
