package logger

import (
	"encoding/json"
	"net/http"

	"github.com/sourcenetwork/corelog"
)

// LevelPath is where LevelHandler is registered on the pprof listener.
const LevelPath = "/debug/loglevel"

// LevelHandler reports and sets DefraDB's log level at runtime.
//
// GET returns the current level. POST or PUT with a "level" query or form value
// sets it and returns the new one. Only "info" and "error" are accepted.
//
// This works without a restart because corelog re-reads its config on every record
// rather than caching a leveler, so a SetConfig lands on the next line logged. The
// LOG_LEVEL environment variable is read once at corelog's package init, so it is
// only ever the starting value.
//
// Scope: this moves DefraDB's output only. The host's own logs go through zap at a
// level fixed when Init runs, so they are unaffected either way.
func LevelHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeLevel(w, http.StatusOK, corelog.GetConfig("").Level)

	case http.MethodPost, http.MethodPut:
		level := r.FormValue("level")
		// corelog maps an unrecognised level to info rather than rejecting it, so an
		// unchecked value here would report a level the loggers are not using.
		if level != corelog.LevelInfo && level != corelog.LevelError {
			http.Error(w, `level must be "info" or "error"`, http.StatusBadRequest)
			return
		}
		// Read-modify-write so format, output and the stack trace and source flags
		// survive a level change.
		cfg := corelog.GetConfig("")
		cfg.Level = level
		corelog.SetConfig(cfg)
		writeLevel(w, http.StatusOK, level)

	default:
		w.Header().Set("Allow", "GET, POST, PUT")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func writeLevel(w http.ResponseWriter, status int, level string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"level": level})
}
