//go:build hostplayground

package playground

import (
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/playground"
)

// NewServer creates a new HTTP server that serves the playground UI
// and proxies GraphQL requests to the defradb API.
func NewServer(defraAPIURL string) (http.Handler, error) {
	mux := http.NewServeMux()

	// Parse the defradb API URL for proxying
	// Ensure it has a scheme
	apiURL := defraAPIURL
	if !strings.HasPrefix(apiURL, "http://") && !strings.HasPrefix(apiURL, "https://") {
		apiURL = "http://" + apiURL
	}

	defraURL, err := url.Parse(apiURL)
	if err != nil {
		return nil, err
	}

	// Create a reverse proxy for the single public GraphQL query endpoint.
	// DefraDB exposes additional administrative and mutation-capable API routes;
	// those must remain reachable only on the private 9181 listener.
	proxy := httputil.NewSingleHostReverseProxy(defraURL)

	mux.HandleFunc("/api/v0/graphql", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		// Update the request URL to point to defradb
		r.URL.Scheme = defraURL.Scheme
		r.URL.Host = defraURL.Host
		proxy.ServeHTTP(w, r)
	})
	mux.HandleFunc("/api/", http.NotFound)

	// Proxy health-check
	mux.HandleFunc("/health-check", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Scheme = defraURL.Scheme
		r.URL.Host = defraURL.Host
		proxy.ServeHTTP(w, r)
	})

	// Proxy openapi.json
	mux.HandleFunc("/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Scheme = defraURL.Scheme
		r.URL.Host = defraURL.Host
		proxy.ServeHTTP(w, r)
	})

	// Serve playground static files
	sub, err := fs.Sub(playground.Dist, "dist")
	if err != nil {
		return nil, err
	}
	fileServer := http.FileServer(http.FS(sub))

	// Serve playground at root. API paths are handled by the more-specific
	// routes above and never fall through to the static file server.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health-check" ||
			r.URL.Path == "/openapi.json" {
			r.URL.Scheme = defraURL.Scheme
			r.URL.Host = defraURL.Host
			proxy.ServeHTTP(w, r)
			return
		}
		// Otherwise serve the playground
		fileServer.ServeHTTP(w, r)
	})

	return mux, nil
}
