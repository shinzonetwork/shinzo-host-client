package host

import (
	"encoding/json"
	"net/http"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func mountSystemStats(srv *hostserver.Server, cfg *config.Config) {
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

	mux := srv.Mux()
	mux.HandleFunc("/api/system", handler)
	mux.HandleFunc("/api/system/", handler)
}
