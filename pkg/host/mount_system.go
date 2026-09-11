package host

import (
	"encoding/json"
	"net/http"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func mountSystemStats(srv *hostserver.Server, cfg *hostconfig.Config) {
	handler := func(w http.ResponseWriter, _ *http.Request) {
		stats, err := collectSystemStats(cfg.Store.Path)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(stats)
	}

	// Both forms: mountGraphQL owns the "/api/" subtree, so a request to
	// /api/system/ (trailing slash) would otherwise fall through to it
	// instead of here.
	mux := srv.Mux()
	mux.HandleFunc("/api/system", handler)
	mux.HandleFunc("/api/system/", handler)
}
