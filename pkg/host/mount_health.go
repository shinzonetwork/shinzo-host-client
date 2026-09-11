package host

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

func mountHealth(srv *hostserver.Server, defra DefraService) {
	startedAt := time.Now()

	srv.Mux().HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		snapshot := defra.Metrics().GetSnapshot()
		healthy := defra.DB() != nil
		uptime := time.Since(startedAt)

		status := "healthy"
		if !healthy {
			status = "unhealthy"
		}

		resp := server.HealthResponse{
			Status:           status,
			Timestamp:        time.Now(),
			CurrentBlock:     int64(snapshot.MostRecentBlock), //nolint:gosec // block numbers fit in int64
			LastProcessed:    snapshot.LastDocumentTime,
			DefraDBConnected: healthy,
			Uptime:           uptime.String(),
			UptimeSeconds:    uptime.Seconds(),
			P2P:              collectPeerInfo(r.Context(), defra.DB()),
		}

		w.Header().Set("Content-Type", "application/json")
		if !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
}

func collectPeerInfo(ctx context.Context, db node.DB) *server.P2PInfo {
	if db == nil {
		return &server.P2PInfo{}
	}
	info := &server.P2PInfo{Enabled: true}

	if ownAddrs, err := db.PeerInfo(ctx); err == nil {
		if ownPeers, _ := defradb.BootstrapIntoPeers(ownAddrs); len(ownPeers) > 0 {
			var addrs []string
			for _, p := range ownPeers {
				addrs = append(addrs, p.Addresses...)
			}
			info.Self = &server.PeerInfo{ID: ownPeers[0].ID, Addresses: addrs}
		}
	}

	activeAddrs, err := db.ActivePeers(ctx)
	if err != nil {
		return info
	}
	activePeers, _ := defradb.BootstrapIntoPeers(activeAddrs)

	seen := make(map[string]*server.PeerInfo, len(activePeers))
	for _, p := range activePeers {
		if existing, ok := seen[p.ID]; ok {
			existing.Addresses = append(existing.Addresses, p.Addresses...)
		} else {
			seen[p.ID] = &server.PeerInfo{ID: p.ID, Addresses: p.Addresses}
		}
	}
	for _, p := range seen {
		info.PeerInfo = append(info.PeerInfo, *p)
	}
	return info
}
