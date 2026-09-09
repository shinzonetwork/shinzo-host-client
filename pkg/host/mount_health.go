package host

import "net/http"

// mountHealth registers a basic liveness check directly on mux. hostserver
// doesn't know this exists, it just serves whatever's mounted on its mux.
func mountHealth(mux *http.ServeMux) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
}
