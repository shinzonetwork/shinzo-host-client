//go:build !hostplayground

package playground

import "net/http"

// NewServer returns a no-op handler when playground is not enabled
func NewServer(defraAPIURL string) (http.Handler, error) {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}), nil
}

