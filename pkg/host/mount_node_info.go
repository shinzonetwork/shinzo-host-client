package host

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

type nodeInfo struct {
	NodeID string `json:"node_id"`
	PeerID string `json:"peer_id"`
	DID    string `json:"did"`
}

func mountNodeInfo(srv *hostserver.Server, keys NodeKeys) error {
	peerID, err := keys.PeerID()
	if err != nil {
		return fmt.Errorf("resolving peer id: %w", err)
	}

	body, err := json.Marshal(nodeInfo{
		NodeID: keys.OperatorAddress().Hex(),
		PeerID: peerID.String(),
		DID:    keys.DID(),
	})
	if err != nil {
		return fmt.Errorf("encoding node info: %w", err)
	}

	handler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}

	mux := srv.Mux()
	mux.HandleFunc("/api/node", handler)
	mux.HandleFunc("/api/node/", handler)

	return nil
}
