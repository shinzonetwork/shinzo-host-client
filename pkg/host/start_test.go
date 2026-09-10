package host

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

func testHostConfig() *hostconfig.Config {
	cfg := hostconfig.Default()
	cfg.HTTP.Addr = ":0"
	return &cfg
}

// fakeDefraService lets Start's own orchestration (does it call Start,
// mount ordering, shutdown wiring) be tested without a real embedded
// database. DB/Options return nil, fine for anything that doesn't actually
// execute a query, a real round trip is covered separately in
// TestStart_ServesGraphQLAndHealthOnSamePort.
type fakeDefraService struct {
	started bool
	stopped bool
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

func TestStart_FailsFastOnInvalidACPConfig(t *testing.T) {
	cfg := testHostConfig()
	cfg.ACP.Enabled = true // missing chain_id/epoch_length/min_query_balance

	// nil defra: ACP validation fails before Start ever touches it.
	_, err := Start(context.Background(), cfg, zap.NewNop(), NodeKeys{}, nil)
	if err == nil {
		t.Fatal("expected Start to fail on an invalid acp config, got nil")
	}
	if !strings.Contains(err.Error(), "acp config") {
		t.Fatalf(`expected the error to be wrapped as "acp config: ...", got: %v`, err)
	}
}

func TestStart_MountsEverythingWithoutRealDefra(t *testing.T) {
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

	base := "http://" + srv.Addr()
	for _, path := range []string{"/health", "/api/node", "/console"} {
		resp, err := http.Get(base + path) //nolint:noctx // test
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected %s 200, got %d", path, resp.StatusCode)
		}
	}

	if err := srv.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !fake.stopped {
		t.Fatal("expected Close to stop the defra service too")
	}
}

func TestStart_StopsDefraIfAMountFailsAfterDefraStarted(t *testing.T) {
	cfg := testHostConfig()
	keys := NodeKeys{PeerKeySeed: []byte("short")}
	fake := &fakeDefraService{}

	_, err := Start(context.Background(), cfg, zap.NewNop(), keys, fake)
	if err == nil {
		t.Fatal("expected Start to fail deriving the peer id for /api/node")
	}
	if !fake.started {
		t.Fatal("expected defra.Start to have been called before the failing mount")
	}
	if !fake.stopped {
		t.Fatal("expected Start to stop defra itself after a later mount failed")
	}
}

func TestStart_ServesGraphQLAndHealthOnSamePort(t *testing.T) {
	cfg := testKeysConfig(t)
	cfg.P2P.Enabled = false
	cfg.Store.Path = filepath.Join(cfg.Node.DataDir, "data")

	keys, err := deriveKeys(testMnemonic(t))
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}

	defra := NewDefraService(cfg, zap.NewNop(), keys.IdentityKey, keys.PeerKeySeed)

	srv, err := Start(context.Background(), cfg, zap.NewNop(), keys, defra)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := srv.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	base := "http://" + srv.Addr()

	healthResp, err := http.Get(base + "/health") //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer healthResp.Body.Close() //nolint:errcheck // test
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /health 200, got %d", healthResp.StatusCode)
	}

	query := `{ ` + constants.CollectionBlock + ` { __typename } }`
	gqlURL := base + "/api/v0/graphql?query=" + url.QueryEscape(query)
	gqlResp, err := http.Get(gqlURL) //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /api/v0/graphql: %v", err)
	}
	defer gqlResp.Body.Close() //nolint:errcheck // test
	body, _ := io.ReadAll(gqlResp.Body)
	if gqlResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /api/v0/graphql 200, got %d: %s", gqlResp.StatusCode, body)
	}
	if !strings.Contains(string(body), constants.CollectionBlock) {
		t.Fatalf("expected a graphql response echoing %s, got: %s", constants.CollectionBlock, body)
	}

	nodeResp, err := http.Get(base + "/api/node") //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /api/node: %v", err)
	}
	defer nodeResp.Body.Close() //nolint:errcheck // test
	nodeBody, _ := io.ReadAll(nodeResp.Body)
	if nodeResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /api/node 200, got %d: %s", nodeResp.StatusCode, nodeBody)
	}
	peerID, err := keys.PeerID()
	if err != nil {
		t.Fatalf("PeerID: %v", err)
	}
	for _, want := range []string{keys.OperatorAddress().Hex(), peerID.String(), keys.DID()} {
		if !strings.Contains(string(nodeBody), want) {
			t.Fatalf("expected /api/node to contain %q, got: %s", want, nodeBody)
		}
	}

	// http.Get follows the /console -> /console/ redirect automatically.
	consoleResp, err := http.Get(base + "/console") //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /console: %v", err)
	}
	defer consoleResp.Body.Close() //nolint:errcheck // test
	if consoleResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /console 200, got %d", consoleResp.StatusCode)
	}

	assetResp, err := http.Get(base + "/console/assets/app.js") //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /console/assets/app.js: %v", err)
	}
	defer assetResp.Body.Close() //nolint:errcheck // test
	if assetResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /console/assets/app.js 200, got %d", assetResp.StatusCode)
	}
}

func TestBuildACPConfig_DisabledIsValid(t *testing.T) {
	acpCfg, err := buildACPConfig(testHostConfig())
	if err != nil {
		t.Fatalf("expected a disabled acp config to be valid, got: %v", err)
	}
	if acpCfg.Enabled {
		t.Fatal("expected Enabled to default to false")
	}
}

func TestBuildACPConfig_ChainIDComesFromShinzo(t *testing.T) {
	cfg := testHostConfig()
	cfg.Shinzo.ChainID = 12345
	cfg.ACP.Enabled = true
	cfg.ACP.MinQueryBalance = "1000"
	cfg.ACP.EpochLength = 100

	acpCfg, err := buildACPConfig(cfg)
	if err != nil {
		t.Fatalf("buildACPConfig: %v", err)
	}
	if acpCfg.ChainID != 12345 {
		t.Fatalf("expected ChainID 12345 pulled from shinzo.chain_id, got %d", acpCfg.ChainID)
	}
}

func TestBuildACPConfig_InvalidAttesterWindowErrors(t *testing.T) {
	cfg := testHostConfig()
	cfg.ACP.AttesterWindow = "not-a-duration"

	if _, err := buildACPConfig(cfg); err == nil {
		t.Fatal("expected an error for a malformed attester_window, got nil")
	}
}
