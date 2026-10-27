package host

import (
	"encoding/json"
	"net/http"
	"time"
)

type statusResponse struct {
	Status string `json:"status"`
	Uptime string `json:"uptime"`
}

func mountStatus(mux *http.ServeMux, startedAt time.Time) {
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(statusResponse{
			Status: "ok",
			Uptime: time.Since(startedAt).String(),
		})
	})
}
