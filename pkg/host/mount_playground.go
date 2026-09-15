package host

import (
	"fmt"
	"io/fs"
	"net/http"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
	"github.com/shinzonetwork/shinzo-host-client/playground"
)

func mountPlayground(srv *hostserver.Server, cfg *config.Config) error {
	if !cfg.Playground.Enabled {
		return nil
	}

	assets, err := fs.Sub(playground.Dist, "dist/assets")
	if err != nil {
		return fmt.Errorf("playground assets: %w", err)
	}
	index, err := playground.Dist.ReadFile("dist/index.html")
	if err != nil {
		return fmt.Errorf("playground index: %w", err)
	}

	mux := srv.Mux()

	mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(assets))))

	serveIndex := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	}
	mux.HandleFunc("/playground", serveIndex)
	mux.HandleFunc("/playground/", serveIndex)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/playground", http.StatusFound)
	})

	return nil
}
