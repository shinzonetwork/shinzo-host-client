package host

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
)

// The documents of every generator that answers are merged, each ID once, and a generator that does
// not answer is skipped; the listing fails only when no generator answers.
func TestGeneratorDocuments(t *testing.T) {
	serve := func(listed map[string][]string) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != documentsEndpoint || r.URL.Query().Get("from") != "10" || r.URL.Query().Get("to") != "12" {
				http.Error(w, "unexpected request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(listed)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	a := serve(map[string][]string{"Header": {"h1"}, "BlockSignature": {"s1"}})
	b := serve(map[string][]string{"Header": {"h1"}, "BlockSignature": {"s2"}})
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)

	cases := []struct {
		name       string
		generators []string
		want       map[string][]string
	}{
		{name: "merged across generators", generators: []string{a, b}, want: map[string][]string{"Header": {"h1"}, "BlockSignature": {"s1", "s2"}}},
		{name: "a generator that does not answer is skipped", generators: []string{down.URL, a}, want: map[string][]string{"Header": {"h1"}, "BlockSignature": {"s1"}}},
		{name: "no generator answers", generators: []string{down.URL}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var generators []chain.Generator
			for _, url := range tc.generators {
				generators = append(generators, chain.Generator{URL: url})
			}

			got, err := generatorDocuments(generators, http.DefaultClient)(context.Background(), 10, 12)

			if tc.want == nil {
				require.ErrorIs(t, err, errDocumentsRefused)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
