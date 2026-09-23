package host

import (
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/console"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func mountConsole(srv *hostserver.Server) error {
	sub, err := fs.Sub(console.Dist, "dist")
	if err != nil {
		return fmt.Errorf("console assets: %w", err)
	}

	index, err := console.Dist.ReadFile("dist/index.html")
	if err != nil {
		return fmt.Errorf("console index: %w", err)
	}

	files := http.StripPrefix("/console/", http.FileServer(http.FS(sub)))
	mux := srv.Mux()

	mux.HandleFunc("/console/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/console/")
		if rel != "" {
			if f, err := sub.Open(rel); err == nil {
				info, statErr := f.Stat()
				_ = f.Close()
				if statErr == nil && !info.IsDir() {
					files.ServeHTTP(w, r)
					return
				}
			}
			if strings.Contains(path.Base(rel), ".") {
				http.NotFound(w, r)
				return
			}
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	})
	mux.HandleFunc("/console", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/console/", http.StatusFound)
	})

	return nil
}
