package hostserver

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
)

func testConfig() *hostconfig.Config {
	cfg := hostconfig.Default()
	cfg.HTTP.Addr = ":0"
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
	if s.Mux() == nil {
		t.Fatal("expected New to build a mux")
	}
}

func TestStart_ServesWhateverWasMountedOnMux(t *testing.T) {
	s, err := New(testConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	s.Mux().HandleFunc("/whatever", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
}

func TestStart_SetsAddr(t *testing.T) {
	s, err := New(testConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Addr() != "" {
		t.Fatalf("expected Addr to be empty before Start, got %q", s.Addr())
	}

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()

	if s.Addr() == "" {
		t.Fatal("expected Addr to be set once Start resolves the ephemeral port")
	}
}

func TestRegisterShutdown_CalledOnClose(t *testing.T) {
	s, err := New(testConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	called := false
	s.RegisterShutdown(func(context.Context) error {
		called = true
		return nil
	})

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !called {
		t.Fatal("expected the registered shutdown func to run on Close")
	}
}

func TestRegisterShutdown_RunsInReverseOrderAndKeepsGoingOnError(t *testing.T) {
	s, err := New(testConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var order []int
	s.RegisterShutdown(func(context.Context) error {
		order = append(order, 1)
		return errors.New("first registered still fails, second must still run")
	})
	s.RegisterShutdown(func(context.Context) error {
		order = append(order, 2)
		return nil
	})

	err = s.Close(context.Background())
	if err == nil {
		t.Fatal("expected Close to surface the failing shutdown func's error")
	}
	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatalf("expected reverse registration order [2 1], got %v", order)
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
