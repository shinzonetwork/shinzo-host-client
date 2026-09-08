package hostserver

import (
	"context"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
)

func testConfig() *hostconfig.Config {
	cfg := hostconfig.Default()
	cfg.HTTP.Addr = ":0" // ephemeral port, tests never fight over a fixed one
	return &cfg
}

func TestNew_NilConfigErrors(t *testing.T) {
	if _, err := New(nil, zap.NewNop()); err == nil {
		t.Fatal("expected an error for a nil config, got nil")
	}
}

func TestNew_NilLoggerErrors(t *testing.T) {
	if _, err := New(testConfig(), nil); err == nil {
		t.Fatal("expected an error for a nil logger, got nil")
	}
}

func TestNew_Succeeds(t *testing.T) {
	s, err := New(testConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.mux == nil {
		t.Fatal("expected New to build a mux")
	}
}

func TestMountHealth_ServesOK(t *testing.T) {
	s, err := New(testConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.mountHealth()

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)

	if rec.Code != 200 { //nolint:mnd
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestMountPlayground_DisabledIsNoop(t *testing.T) {
	cfg := testConfig()
	cfg.Playground.Enabled = false

	s, err := New(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := s.mountPlayground(); err != nil {
		t.Fatalf("expected mountPlayground to no-op when disabled, got: %v", err)
	}
}

func TestClose_BeforeStartIsSafe(t *testing.T) {
	s, err := New(testConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("expected Close before Start to be a safe no-op, got: %v", err)
	}
}
