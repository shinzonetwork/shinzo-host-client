package host

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func TestMountNodeInfo_ServesKeys(t *testing.T) {
	srv, err := hostserver.New(testHostConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("hostserver.New: %v", err)
	}

	keys, err := deriveKeys(testMnemonic(t))
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}

	if err := mountNodeInfo(srv, keys); err != nil {
		t.Fatalf("mountNodeInfo: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/node", nil)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var got nodeInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshaling response: %v", err)
	}

	peerID, err := keys.PeerID()
	if err != nil {
		t.Fatalf("PeerID: %v", err)
	}
	if got.NodeID != keys.OperatorAddress().Hex() {
		t.Fatalf("expected node_id %s, got %s", keys.OperatorAddress().Hex(), got.NodeID)
	}
	if got.PeerID != peerID.String() {
		t.Fatalf("expected peer_id %s, got %s", peerID.String(), got.PeerID)
	}
	if got.DID != keys.DID() {
		t.Fatalf("expected did %s, got %s", keys.DID(), got.DID)
	}
}
