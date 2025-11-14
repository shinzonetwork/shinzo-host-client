//go:build hostplayground

package playground

import (
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/shinzonetwork/host/playground"
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

	// Create a reverse proxy for GraphQL API requests
	proxy := httputil.NewSingleHostReverseProxy(defraURL)

	// Proxy all API requests to defradb
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		// Update the request URL to point to defradb
		r.URL.Scheme = defraURL.Scheme
		r.URL.Host = defraURL.Host
		proxy.ServeHTTP(w, r)
	})

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
	
	// Serve playground at root, but only if it's not an API request
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// If it's an API request, proxy it
		if strings.HasPrefix(r.URL.Path, "/api/") || 
		   r.URL.Path == "/health-check" || 
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

