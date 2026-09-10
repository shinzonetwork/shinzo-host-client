package host

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func TestMountConsole_RedirectsBareConsolePath(t *testing.T) {
	srv, err := hostserver.New(testHostConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}
	if err := mountConsole(srv); err != nil {
		t.Fatalf("mountConsole: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/console", nil)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected a redirect to /console/, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/console/" {
		t.Fatalf("expected redirect to /console/, got %q", loc)
	}
}

func TestMountConsole_ServesIndexAndAssets(t *testing.T) {
	srv, err := hostserver.New(testHostConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}
	if err := mountConsole(srv); err != nil {
		t.Fatalf("mountConsole: %v", err)
	}

	index := httptest.NewRequest(http.MethodGet, "/console/", nil)
	indexRec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(indexRec, index)

	if indexRec.Code != http.StatusOK {
		t.Fatalf("expected /console/ 200, got %d", indexRec.Code)
	}
	if ct := indexRec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("expected text/html content type, got %q", ct)
	}
	if !strings.Contains(indexRec.Body.String(), "assets/app.js") {
		t.Fatal("expected the console index to reference assets/app.js")
	}

	asset := httptest.NewRequest(http.MethodGet, "/console/assets/app.js", nil)
	assetRec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(assetRec, asset)

	if assetRec.Code != http.StatusOK {
		t.Fatalf("expected /console/assets/app.js 200, got %d", assetRec.Code)
	}
	if !strings.Contains(assetRec.Body.String(), "/api/node") {
		t.Fatal("expected app.js to fetch /api/node")
	}
}
