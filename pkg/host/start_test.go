package host

import (
	"context"
	"net/http"
	"testing"

	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

func testHostConfig() *hostconfig.Config {
	cfg := hostconfig.Default()
	cfg.HTTP.Addr = ":0"
	return &cfg
}

type fakeDefraService struct {
	started bool
	stopped bool
	metrics *server.HostMetrics
}

func (f *fakeDefraService) Start(context.Context) error {
	f.started = true
	return nil
}

func (f *fakeDefraService) Stop(context.Context) error {
	f.stopped = true
	return nil
}

func (f *fakeDefraService) DB() node.DB                   { return nil }
func (f *fakeDefraService) Options() *options.NodeOptions { return nil }

func (f *fakeDefraService) Metrics() *server.HostMetrics {
	if f.metrics == nil {
		f.metrics = server.NewHostMetrics()
	}
	return f.metrics
}

func TestStart_StartsDefraAndTheHTTPServer(t *testing.T) {
	cfg := testHostConfig()
	keys, err := deriveKeys(testMnemonic(t))
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}

	fake := &fakeDefraService{}

	srv, err := Start(context.Background(), cfg, zap.NewNop(), keys, fake)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !fake.started {
		t.Fatal("expected Start to call defra.Start")
	}

	resp, err := http.Get("http://" + srv.Addr() + "/") //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()

	if err := srv.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !fake.stopped {
		t.Fatal("expected Close to stop the defra service too")
	}
}

func TestStart_StopsDefraIfTheHTTPServerFailsToBind(t *testing.T) {
	cfg := testHostConfig()
	cfg.HTTP.Addr = "not-a-valid-address"
	fake := &fakeDefraService{}

	_, err := Start(context.Background(), cfg, zap.NewNop(), NodeKeys{}, fake)
	if err == nil {
		t.Fatal("expected Start to fail binding the http server")
	}
	if !fake.started {
		t.Fatal("expected defra.Start to have been called before the failing bind")
	}
	if !fake.stopped {
		t.Fatal("expected Start to stop defra itself after the http server failed to bind")
	}
}
