package host

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

// this only proves the route and shutdown are wired, not that
// a query round-trips, that needs a real node, see
// TestStart_ServesGraphQLAndHealthOnSamePort.
func TestMountGraphQL_RegistersAPIRouteAndShutdown(t *testing.T) {
	srv, err := hostserver.New(testHostConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}

	if err := mountGraphQL(srv, nil, nil); err != nil {
		t.Fatalf("mountGraphQL: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v0/graphql", nil)
	_, pattern := srv.Mux().Handler(req)
	if pattern != "/api/" {
		t.Fatalf("expected /api/v0/graphql to route through the /api/ mount, got pattern %q", pattern)
	}

	if err := srv.Close(context.Background()); err != nil {
		t.Fatalf("expected the registered graphql handler shutdown to run cleanly, got: %v", err)
	}
}
