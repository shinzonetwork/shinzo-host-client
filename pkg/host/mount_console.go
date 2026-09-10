package host

import (
	"fmt"
	"io/fs"
	"net/http"

	"github.com/shinzonetwork/shinzo-host-client/console"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func mountConsole(srv *hostserver.Server) error {
	sub, err := fs.Sub(console.Dist, "dist")
	if err != nil {
		return fmt.Errorf("console assets: %w", err)
	}

	fileServer := http.FileServer(http.FS(sub))
	mux := srv.Mux()
	mux.Handle("/console/", http.StripPrefix("/console/", fileServer))
	mux.HandleFunc("/console", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/console/", http.StatusFound)
	})

	return nil
}
